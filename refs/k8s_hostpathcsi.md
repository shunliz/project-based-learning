# 从零到一：Kubernetes HostPath CSI 驱动程序完整实现指南

## 📖 前言

本文将详细介绍如何从零开始实现一个完整的 Kubernetes HostPath CSI (Container Storage Interface) 驱动程序，包括代码实现、容器化、部署和测试的全过程。通过本文，您将学会：

- CSI 接口的完整实现
- Go 语言开发 Kubernetes 存储插件
- Docker 容器化和 Kubernetes 部署
- 存储卷的动态供应和管理

## 🎯 项目目标

实现一个生产级的 HostPath CSI 驱动程序，支持：

✅ 动态卷供应 (Dynamic Provisioning)
✅ 卷挂载和卸载 (Mount/Unmount)
✅ 数据持久化存储
✅ Kubernetes 标准 CSI 接口
✅ 完整的错误处理和日志记录

## 🏗️ 架构设计

CSI 组件架构
┌─────────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐    ┌─────────────────────────────────────┐ │
│  │  CSI Controller │    │         CSI Node (DaemonSet)        │ │
│  │   (Deployment)  │    │                                     │ │
│  │                 │    │ ┌─────────────┐ ┌─────────────────┐ │ │
│  │ ┌─────────────┐ │    │ │node-driver- │ │  hostpath-csi   │ │ │
│  │ │csi-provisioner│    │ │registrar    │ │     driver      │ │ │
│  │ └─────────────┘ │    │ └─────────────┘ └─────────────────┘ │ │
│  │ ┌─────────────┐ │    │ ┌─────────────────────────────────┐ │ │
│  │ │hostpath-csi │ │    │ │      liveness-probe             │ │ │
│  │ │ controller  │ │    │ └─────────────────────────────────┘ │ │
│  │ └─────────────┘ │    └─────────────────────────────────────┘ │
│  └─────────────────┘                                            │
└─────────────────────────────────────────────────────────────────┘

## 📁 项目结构

首先创建完整的项目目录结构：

mkdir -p hostpath-csi/{cmd/hostpath-csi,pkg/hostpath,deploy/{minikube,production},test,docs}
cd hostpath-csi

hostpath-csi/
├── cmd/hostpath-csi/           # 主程序入口
│   └── main.go
├── pkg/hostpath/               # 核心业务逻辑
│   ├── driver.go               # CSI 驱动程序主体
│   ├── identity.go             # Identity 服务实现
│   ├── controller.go           # Controller 服务实现
│   └── node.go                 # Node 服务实现
├── deploy/                     # 部署配置
│   ├── minikube/               # 开发环境
│   └── production/             # 生产环境
├── test/                       # 测试文件
├── docs/                       # 文档
├── Dockerfile                  # 容器构建
├── Makefile                    # 构建脚本
├── go.mod                      # Go 模块
└── README.md                   # 项目说明

## **🔧第一步：初始化 Go 模块**

### 1. 创建 go.mod 文件

```
cat > go.mod << 'EOF'
module hostpath-csi

go 1.22

require (
    github.com/container-storage-interface/spec v1.9.0
    github.com/google/uuid v1.3.0
    google.golang.org/grpc v1.58.3
    k8s.io/klog/v2 v2.100.1
    k8s.io/utils v0.0.0-20230726121419-3b25d923346b
)

require (
    github.com/golang/protobuf v1.5.3 // indirect
    golang.org/x/net v0.13.0 // indirect
    golang.org/x/sys v0.10.0 // indirect
    golang.org/x/text v0.11.0 // indirect
    google.golang.org/genproto/googleapis/rpc v0.0.0-20230711160842-782d3b101e98 // indirect
    google.golang.org/protobuf v1.31.0 // indirect
)
EOF
```

### 2. 下载依赖

go mod tidy

## 💻 第二步：核心代码实现

1. 主程序入口 (cmd/hostpath-csi/main.go)

```
package main

import (
    "flag"
    "fmt"
    "os"

    "hostpath-csi/pkg/hostpath"
    "k8s.io/klog/v2"

)

var (
    endpoint   = flag.String("endpoint", "unix:///csi/csi.sock", "CSI endpoint")
    nodeID     = flag.String("nodeid", "", "node id")
    driverName = flag.String("drivername", "hostpath.csi.example.com", "name of the driver")
    version    = flag.String("version", "v0.1.0", "version of the driver")
)

func main() {
    klog.InitFlags(nil)
    flag.Parse()

    if *nodeID == "" {
        klog.Error("nodeid must be provided")
        os.Exit(1)
    }
    
    driver, err := hostpath.NewDriver(*driverName, *version, *nodeID)
    if err != nil {
        klog.Errorf("Failed to initialize driver: %v", err)
        os.Exit(1)
    }
    
    klog.Infof("Driver: %s version: %s", *driverName, *version)
    
    if err := driver.Run(*endpoint); err != nil {
        klog.Errorf("Failed to run driver: %v", err)
        os.Exit(1)
    }

}
```

2. CSI 驱动程序主体 (pkg/hostpath/driver.go)

  ```
  package hostpath
  
  import (
      "context"
      "net"
      "net/url"
      "os"
      "path"
      "path/filepath"
  
      "github.com/container-storage-interface/spec/lib/go/csi"
      "google.golang.org/grpc"
      "k8s.io/klog/v2"
      "k8s.io/utils/mount"
  
  )
  
  type Driver struct {
      name    string
      version string
      nodeID  string
  
      srv     *grpc.Server
      mounter mount.Interface
  
  }
  
  func NewDriver(driverName, version, nodeID string) (*Driver, error) {
      if driverName == "" {
          return nil, fmt.Errorf("driver name missing")
      }
      if version == "" {
          return nil, fmt.Errorf("driver version missing")
      }
      if nodeID == "" {
          return nil, fmt.Errorf("nodeID missing")
      }
  
      klog.Infof("Driver: %v version: %v", driverName, version)
      
      return &Driver{
          name:    driverName,
          version: version,
          nodeID:  nodeID,
          mounter: mount.New(""),
      }, nil
  
  }
  
  func (d *Driver) Run(endpoint string) error {
      u, err := url.Parse(endpoint)
      if err != nil {
          return fmt.Errorf("unable to parse address: %q", endpoint)
      }
  
      addr := path.Join(u.Host, filepath.FromSlash(u.Path))
      if u.Host == "" {
          addr = filepath.FromSlash(u.Path)
      }
      
      // Remove existing socket file
      if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
          return fmt.Errorf("failed to remove unix domain socket file %s: %v", addr, err)
      }
      
      listener, err := net.Listen(u.Scheme, addr)
      if err != nil {
          return fmt.Errorf("failed to listen: %v", err)
      }
      
      // Create gRPC server
      d.srv = grpc.NewServer()
      
      // Register CSI services
      csi.RegisterIdentityServer(d.srv, NewIdentityServer(d))
      csi.RegisterControllerServer(d.srv, NewControllerServer(d))
      csi.RegisterNodeServer(d.srv, NewNodeServer(d))
      
      klog.Infof("Listening for connections on address: %#v", listener.Addr())
      
      return d.srv.Serve(listener)
  
  }
  
  func (d *Driver) Stop() {
      klog.Infof("Stopping server")
      d.srv.Stop()
  }
  ```

  

3. Identity 服务实现 (pkg/hostpath/identity.go)

  ```
  package hostpath
  
  import (
      "context"
  
      "github.com/container-storage-interface/spec/lib/go/csi"
  
  )
  
  type IdentityServer struct {
      driver *Driver
      csi.UnimplementedIdentityServer
  }
  
  func NewIdentityServer(d *Driver) csi.IdentityServer {
      return &IdentityServer{
          driver: d,
      }
  }
  
  func (is *IdentityServer) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
      return &csi.GetPluginInfoResponse{
          Name:          is.driver.name,
          VendorVersion: is.driver.version,
      }, nil
  }
  
  func (is *IdentityServer) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
      return &csi.GetPluginCapabilitiesResponse{
          Capabilities: []*csi.PluginCapability{
              {
                  Type: &csi.PluginCapability_Service_{
                      Service: &csi.PluginCapability_Service{
                          Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
                      },
                  },
              },
          },
      }, nil
  }
  
  func (is *IdentityServer) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
      return &csi.ProbeResponse{}, nil
  }
  ```

  

4. Controller 服务实现 (pkg/hostpath/controller.go)

  ```
  package hostpath
  
  import (
      "context"
      "fmt"
      "os"
      "path/filepath"
  
      "github.com/container-storage-interface/spec/lib/go/csi"
      "github.com/google/uuid"
      "google.golang.org/grpc/codes"
      "google.golang.org/grpc/status"
      "k8s.io/klog/v2"
  
  )
  
  const (
      // hostpathBase 是在主机上存储卷数据的基础目录
      hostpathBase = "/var/lib/hostpath-csi"
  )
  
  type ControllerServer struct {
      driver *Driver
      csi.UnimplementedControllerServer
  }
  
  func NewControllerServer(d *Driver) csi.ControllerServer {
      return &ControllerServer{
          driver: d,
      }
  }
  
  func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
      if len(req.GetName()) == 0 {
          return nil, status.Error(codes.InvalidArgument, "Volume name missing in request")
      }
  
      // 生成唯一的卷 ID
      volumeID := uuid.New().String()
      
      // 创建卷目录
      volumePath := filepath.Join(hostpathBase, volumeID)
      if err := os.MkdirAll(volumePath, 0750); err != nil {
          return nil, status.Errorf(codes.Internal, "Failed to create volume directory %s: %v", volumePath, err)
      }
      
      klog.Infof("Created volume %s at path %s", volumeID, volumePath)
      
      // 获取请求的容量
      capacity := req.GetCapacityRange().GetRequiredBytes()
      if capacity == 0 {
          capacity = 1 * 1024 * 1024 * 1024 // 默认 1GB
      }
      
      return &csi.CreateVolumeResponse{
          Volume: &csi.Volume{
              VolumeId:      volumeID,
              CapacityBytes: capacity,
              VolumeContext: req.GetParameters(),
          },
      }, nil
  
  }
  
  func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
      volumeID := req.GetVolumeId()
      if len(volumeID) == 0 {
          return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
      }
  
      volumePath := filepath.Join(hostpathBase, volumeID)
      
      // 删除卷目录
      if err := os.RemoveAll(volumePath); err != nil && !os.IsNotExist(err) {
          return nil, status.Errorf(codes.Internal, "Failed to delete volume directory %s: %v", volumePath, err)
      }
      
      klog.Infof("Deleted volume %s at path %s", volumeID, volumePath)
      
      return &csi.DeleteVolumeResponse{}, nil
  
  }
  
  func (cs *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
      return &csi.ControllerGetCapabilitiesResponse{
          Capabilities: []*csi.ControllerServiceCapability{
              {
                  Type: &csi.ControllerServiceCapability_Rpc{
                      Rpc: &csi.ControllerServiceCapability_RPC{
                          Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
                      },
                  },
              },
          },
      }, nil
  }
  
  func (cs *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
      volumeID := req.GetVolumeId()
      if len(volumeID) == 0 {
          return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
      }
  
      volumePath := filepath.Join(hostpathBase, volumeID)
      if _, err := os.Stat(volumePath); os.IsNotExist(err) {
          return nil, status.Errorf(codes.NotFound, "Volume %s does not exist", volumeID)
      }
      
      return &csi.ValidateVolumeCapabilitiesResponse{
          Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
              VolumeCapabilities: req.GetVolumeCapabilities(),
          },
      }, nil
  
  }
  ```

  

5. Node 服务实现 (pkg/hostpath/node.go)

  ```
  package hostpath
  
  import (
      "context"
      "os"
      "path/filepath"
  
      "github.com/container-storage-interface/spec/lib/go/csi"
      "google.golang.org/grpc/codes"
      "google.golang.org/grpc/status"
      "k8s.io/klog/v2"
      "k8s.io/utils/mount"
  
  )
  
  type NodeServer struct {
      driver  *Driver
      mounter mount.Interface
      csi.UnimplementedNodeServer
  }
  
  func NewNodeServer(d *Driver) csi.NodeServer {
      return &NodeServer{
          driver:  d,
          mounter: mount.New(""),
      }
  }
  
  func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
      volumeID := req.GetVolumeId()
      targetPath := req.GetTargetPath()
  
      if len(volumeID) == 0 {
          return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
      }
      if len(targetPath) == 0 {
          return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
      }
      
      // 构建主机上的卷目录路径
      hostpathDir := filepath.Join(hostpathBase, volumeID)
      
      // 确保主机目录存在
      if err := os.MkdirAll(hostpathDir, 0750); err != nil {
          return nil, status.Errorf(codes.Internal, "Failed to create hostpath dir %s: %v", hostpathDir, err)
      }
      
      // 创建目标挂载点目录
      if err := os.MkdirAll(targetPath, 0750); err != nil {
          return nil, status.Errorf(codes.Internal, "Failed to create target dir %s: %v", targetPath, err)
      }
      
      // 检查是否已经挂载
      notMnt, err := ns.mounter.IsLikelyNotMountPoint(targetPath)
      if err != nil {
          if os.IsNotExist(err) {
              if err := os.MkdirAll(targetPath, 0750); err != nil {
                  return nil, status.Error(codes.Internal, err.Error())
              }
              notMnt = true
          } else {
              return nil, status.Error(codes.Internal, err.Error())
          }
      }
      
      if !notMnt {
          klog.Infof("Volume %s already mounted at %s", volumeID, targetPath)
          return &csi.NodePublishVolumeResponse{}, nil
      }
      
      // 执行绑定挂载
      if err := ns.mounter.Mount(hostpathDir, targetPath, "", []string{"bind"}); err != nil {
          return nil, status.Errorf(codes.Internal, "Failed to mount %s to %s: %v", hostpathDir, targetPath, err)
      }
      
      klog.Infof("Successfully mounted %s to %s", hostpathDir, targetPath)
      
      return &csi.NodePublishVolumeResponse{}, nil
  
  }
  
  func (ns *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
      targetPath := req.GetTargetPath()
      
  
      if len(targetPath) == 0 {
          return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
      }
      
      // 卸载目录
      if err := mount.CleanupMountPoint(targetPath, ns.mounter, true); err != nil {
          return nil, status.Errorf(codes.Internal, "Failed to unmount target path %s: %v", targetPath, err)
      }
      
      klog.Infof("Successfully unmounted %s", targetPath)
      return &csi.NodeUnpublishVolumeResponse{}, nil
  
  }
  
  func (ns *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
      return &csi.NodeGetInfoResponse{
          NodeId: ns.driver.nodeID,
      }, nil
  }
  
  func (ns *NodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
      return &csi.NodeGetCapabilitiesResponse{
          Capabilities: []*csi.NodeServiceCapability{
              // 我们不支持 STAGE_UNSTAGE，所以不声明这个能力
          },
      }, nil
  }
  
  func (ns *NodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
      return nil, status.Error(codes.Unimplemented, "This driver does not support NodeStageVolume")
  }
  
  func (ns *NodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
      return nil, status.Error(codes.Unimplemented, "This driver does not support NodeUnstageVolume")
  }
  
  func (ns *NodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
      return nil, status.Error(codes.Unimplemented, "NodeExpandVolume not implemented")
  }
  
  func (ns *NodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
      return nil, status.Error(codes.Unimplemented, "NodeGetVolumeStats not implemented")
  }
  ```

  

## 🐳 第三步：容器化

1. 创建 Dockerfile

   ```
   # 多阶段构建
   FROM golang:1.22 as builder
   
   WORKDIR /workspace
   
   # 复制 go mod 文件
   COPY go.mod go.sum ./
   RUN go mod download
   
   # 复制源代码
   COPY cmd/ cmd/
   COPY pkg/ pkg/
   
   # 构建二进制文件
   RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -o hostpath-csi ./cmd/hostpath-csi
   
   # 最终镜像
   FROM alpine:3.18
   
   # 安装必要的工具
   RUN apk add --no-cache util-linux
   
   # 复制二进制文件
   COPY --from=builder /workspace/hostpath-csi /hostpath-csi
   
   # 设置入口点
   ENTRYPOINT ["/hostpath-csi"]
   ```


2. 创建 Makefile

```
# Makefile
.PHONY: build docker-build test clean

# 变量定义
IMAGE_NAME ?= hostpath-csi
IMAGE_TAG ?= latest
REGISTRY ?= 

# 构建二进制文件
build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -o bin/hostpath-csi ./cmd/hostpath-csi

# 构建 Docker 镜像
docker-build:
	docker build -t (IMAGE_NAME):(IMAGE_TAG) .

# 推送镜像
docker-push: docker-build
	docker tag (IMAGE_NAME):(IMAGE_TAG) (REGISTRY)/(IMAGE_NAME):(IMAGE_TAG)
	docker push (REGISTRY)/(IMAGE_NAME):(IMAGE_TAG)
```



**运行测试**

test:
	go test -v ./...

**清理**

clean:
	rm -rf bin/
	docker rmi (IMAGE_NAME):(IMAGE_TAG) || true

**格式化代码**

fmt:
	go fmt ./...

**代码检查**

lint:
	golangci-lint run

**构建并部署到 minikube**

minikube-deploy: docker-build
	eval $$(minikube docker-env) && docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .
	kubectl apply -f deploy/minikube/

**清理 minikube 部署**

minikube-clean:
	kubectl delete -f deploy/minikube/ || true

## ☸️ 第四步：Kubernetes 部署配置

1. 命名空间 (deploy/minikube/namespace.yaml)
   
   ```
   apiVersion: v1
   kind: Namespace
   metadata:
    name: hostpath-csi-system
    labels:
    name: hostpath-csi-system
   ```
   
2. RBAC 配置 (deploy/minikube/rbac.yaml)
```
    apiVersion: v1
    kind: ServiceAccount
    metadata:
    name: hostpath-csi-driver
    namespace: hostpath-csi-system

---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: hostpath-csi-driver
rules:
- apiGroups: [""]
  resources: ["nodes"]
  verbs: ["get", "list", "watch"]
- apiGroups: [""]
  resources: ["persistentvolumes"]
  verbs: ["get", "list", "watch", "create", "delete", "patch"]
- apiGroups: [""]
  resources: ["persistentvolumeclaims"]
  verbs: ["get", "list", "watch", "update", "patch"]
- apiGroups: ["storage.k8s.io"]
  resources: ["storageclasses"]
  verbs: ["get", "list", "watch"]
- apiGroups: [""]
  resources: ["events"]
  verbs: ["list", "watch", "create", "update", "patch"]
- apiGroups: ["storage.k8s.io"]
  resources: ["csinodes"]
  verbs: ["get", "list", "watch", "create", "update", "patch"]
- apiGroups: ["storage.k8s.io"]
  resources: ["volumeattachments"]
  verbs: ["get", "list", "watch", "update", "patch"]
- apiGroups: ["storage.k8s.io"]
  resources: ["csidrivers"]
  verbs: ["get", "list", "watch"]

---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: hostpath-csi-driver
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: hostpath-csi-driver
subjects:
- kind: ServiceAccount
  name: hostpath-csi-driver
  namespace: hostpath-csi-system
```

3. CSI 驱动注册 (deploy/minikube/csidriver.yaml)
```
    apiVersion: storage.k8s.io/v1
    kind: CSIDriver
    metadata:
    name: hostpath.csi.example.com
    spec:
    attachRequired: false
    podInfoOnMount: false
    volumeLifecycleModes:
  - Persistent
    fsGroupPolicy: File
```
4. 存储类 (deploy/minikube/storageclass.yaml)
```
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
    name: hostpath-csi
    annotations:
    storageclass.kubernetes.io/is-default-class: "false"
provisioner: hostpath.csi.example.com
volumeBindingMode: Immediate
allowVolumeExpansion: true
reclaimPolicy: Delete
parameters:
  # 可以添加自定义参数
  type: "hostpath"
```

5. Controller 部署 (deploy/minikube/controller.yaml)
```
   apiVersion: apps/v1
   kind: Deployment
   metadata:
    name: hostpath-csi-controller
    namespace: hostpath-csi-system
    labels:
    app: hostpath-csi-controller
   spec:
    replicas: 1
    selector:
    matchLabels:
      app: hostpath-csi-controller
    template:
    metadata:
      labels:
        app: hostpath-csi-controller
    spec:
      serviceAccountName: hostpath-csi-driver
      containers:
      - name: csi-provisioner
        image: registry.k8s.io/sig-storage/csi-provisioner:v3.5.0
        args:
        - --csi-address=$(ADDRESS)
        - --v=2
        - --feature-gates=Topology=true
        - --timeout=150s
        - --leader-election
        - --leader-election-namespace=$(NAMESPACE)
        env:
        - name: ADDRESS
          value: /var/lib/csi/sockets/pluginproxy/csi.sock
        - name: NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
          volumeMounts:
        - name: socket-dir
          mountPath: /var/lib/csi/sockets/pluginproxy/
          resources:
          limits:
            cpu: 200m
            memory: 200Mi
          requests:
            cpu: 100m
            memory: 100Mi

      - name: hostpath-csi-controller
        image: hostpath-csi:latest
        imagePullPolicy: Never
        args:
        - --endpoint=$(CSI_ENDPOINT)
        - --nodeid=$(KUBE_NODE_NAME)
        - --drivername=hostpath.csi.example.com
        - --v=5
        env:
        - name: CSI_ENDPOINT
          value: unix:///var/lib/csi/sockets/pluginproxy/csi.sock
        - name: KUBE_NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
          volumeMounts:
        - name: socket-dir
          mountPath: /var/lib/csi/sockets/pluginproxy/
          resources:
          limits:
            cpu: 200m
            memory: 200Mi
          requests:
            cpu: 100m
            memory: 100Mi

      volumes:
      - name: socket-dir
        emptyDir: {}
```
6. Node DaemonSet (deploy/minikube/daemonset.yaml)
```
   apiVersion: apps/v1
   kind: DaemonSet
   metadata:
    name: hostpath-csi-driver
    namespace: hostpath-csi-system
    labels:
    app: hostpath-csi-driver
   spec:
    selector:
    matchLabels:
      app: hostpath-csi-driver
    template:
    metadata:
      labels:
        app: hostpath-csi-driver
    spec:
      serviceAccountName: hostpath-csi-driver
      hostNetwork: true
      containers:
      - name: node-driver-registrar
        image: registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.8.0
        args:
        - --csi-address=$(ADDRESS)
        - --kubelet-registration-path=$(DRIVER_REG_SOCK_PATH)
        - --v=2
        env:
        - name: ADDRESS
          value: /csi/csi.sock
        - name: DRIVER_REG_SOCK_PATH
          value: /var/lib/kubelet/plugins/hostpath.csi.example.com/csi.sock
          volumeMounts:
        - name: plugin-dir
          mountPath: /csi/
        - name: registration-dir
          mountPath: /registration/
          resources:
          limits:
            cpu: 100m
            memory: 100Mi
          requests:
            cpu: 50m
            memory: 50Mi

      - name: liveness-probe
        image: registry.k8s.io/sig-storage/livenessprobe:v2.10.0
        args:
        - --csi-address=/csi/csi.sock
        - --health-port=9808
        - --v=2
        volumeMounts:
        - name: plugin-dir
          mountPath: /csi/
          resources:
          limits:
            cpu: 100m
            memory: 100Mi
          requests:
            cpu: 50m
            memory: 50Mi

      - name: hostpath-csi-driver
        image: hostpath-csi:latest
        imagePullPolicy: Never
        args:
        - --endpoint=$(CSI_ENDPOINT)
        - --nodeid=$(KUBE_NODE_NAME)
        - --drivername=hostpath.csi.example.com
        - --v=5
        env:
        - name: CSI_ENDPOINT
          value: unix:///csi/csi.sock
        - name: KUBE_NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
          securityContext:
          privileged: true
          volumeMounts:
        - name: plugin-dir
          mountPath: /csi
        - name: pods-mount-dir
          mountPath: /var/lib/kubelet
          mountPropagation: Bidirectional
        - name: hostpath-dir
          mountPath: /var/lib/hostpath-csi
        - name: device-dir
          mountPath: /dev
          ports:
        - containerPort: 9808
          name: healthz
          protocol: TCP
          livenessProbe:
          failureThreshold: 5
          httpGet:
            path: /healthz
            port: healthz
          initialDelaySeconds: 10
          timeoutSeconds: 3
          periodSeconds: 2
          resources:
          limits:
            cpu: 200m
            memory: 200Mi
          requests:
            cpu: 100m
            memory: 100Mi

      volumes:
      - name: registration-dir
        hostPath:
          path: /var/lib/kubelet/plugins_registry/
          type: DirectoryOrCreate
      - name: plugin-dir
        hostPath:
          path: /var/lib/kubelet/plugins/hostpath.csi.example.com
          type: DirectoryOrCreate
      - name: pods-mount-dir
        hostPath:
          path: /var/lib/kubelet
          type: Directory
      - name: hostpath-dir
        hostPath:
          path: /var/lib/hostpath-csi
          type: DirectoryOrCreate
      - name: device-dir
        hostPath:
          path: /dev
          type: Directory
   
      tolerations:
      - operator: Exists
```
## 🧪 第五步：测试用例

1. 测试 PVC (test/test-pvc.yaml)
```
    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
    name: hostpath-test-pvc
    labels:
    app: hostpath-test
    spec:
    accessModes:
  - ReadWriteOnce
    resources:
    requests:
      storage: 1Gi
    storageClassName: hostpath-csi
```
2. 测试 Pod (test/test-pod.yaml)
```
    apiVersion: v1
    kind: Pod
    metadata:
    name: hostpath-test-pod
    labels:
    app: hostpath-test
    spec:
    containers:
  - name: test-container
    image: nginx:alpine
    command:
    - sh
    - -c
    - |
      echo "Testing CSI hostpath volume..." > /data/test.txt
      echo "Current time: $(date)" >> /data/test.txt
      echo "Hostname: $(hostname)" >> /data/test.txt
      cat /data/test.txt
      echo "Starting nginx..."
      nginx -g 'daemon off;'
      volumeMounts:
    - name: hostpath-volume
      mountPath: /data
      ports:
    - containerPort: 80
    volumes:
  - name: hostpath-volume
    persistentVolumeClaim:
      claimName: hostpath-test-pvc
    restartPolicy: Always
```
3. 多 Pod 测试 (test/multi-pod-test.yaml)
```
    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
    name: shared-hostpath-pvc
    spec:
    accessModes:
  - ReadWriteMany
    resources:
    requests:
      storage: 2Gi
    storageClassName: hostpath-csi

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: multi-pod-test
spec:
  replicas: 3
  selector:
    matchLabels:
      app: multi-pod-test
  template:
    metadata:
      labels:
        app: multi-pod-test
    spec:
      containers:
      - name: writer
        image: busybox
        command:
        - sh
        - -c
        - |
          while true; do
            echo "$(date): Message from $(hostname)" >> /shared/messages.log
            sleep 10
          done
          volumeMounts:
        - name: shared-volume
          mountPath: /shared
            volumes:
            - name: shared-volume
          persistentVolumeClaim:
          claimName: shared-hostpath-pvc
```
## 🚀 第六步：完整部署流程

1. 环境准备

**启动 minikube**

minikube start --driver=docker --cpus=2 --memory=4g

**配置 Docker 环境**

eval $(minikube docker-env)

**验证环境**

kubectl cluster-info
2. 构建和部署

**1. 克隆或创建项目**

git clone <your-repo> hostpath-csi
cd hostpath-csi

**2. 构建 Docker 镜像**

make docker-build

**3. 部署到 Kubernetes**

kubectl apply -f deploy/minikube/namespace.yaml
kubectl apply -f deploy/minikube/rbac.yaml
kubectl apply -f deploy/minikube/csidriver.yaml
kubectl apply -f deploy/minikube/storageclass.yaml
kubectl apply -f deploy/minikube/controller.yaml
kubectl apply -f deploy/minikube/daemonset.yaml

**4. 验证部署**

kubectl get pods -n hostpath-csi-system
kubectl get csidriver
kubectl get storageclass

3. 功能测试

**1. 创建测试 PVC**

kubectl apply -f test/test-pvc.yaml

**2. 验证 PVC 状态**

kubectl get pvc hostpath-test-pvc

**3. 创建测试 Pod**

kubectl apply -f test/test-pod.yaml

**4. 验证 Pod 状态**

kubectl get pod hostpath-test-pod

**5. 检查数据**

kubectl exec hostpath-test-pod -- cat /data/test.txt

**6. 验证数据持久化**

kubectl delete pod hostpath-test-pod
kubectl apply -f test/test-pod.yaml
kubectl exec hostpath-test-pod -- cat /data/test.txt

4. 高级测试

**1. 多 Pod 共享测试**

kubectl apply -f test/multi-pod-test.yaml

**2. 查看共享数据**

kubectl exec deployment/multi-pod-test -- tail -f /shared/messages.log

**3. 性能测试**
```
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: performance-test
spec:
  containers:
  - name: fio
    image: ljishen/fio
    command: ["fio"]
    args: 
    - --name=test
    - --rw=randwrite
    - --bs=4k
    - --size=1G
    - --numjobs=1
    - --runtime=60
    - --directory=/data
    volumeMounts:
    - name: test-volume
      mountPath: /data
      volumes:
  - name: test-volume
    persistentVolumeClaim:
      claimName: hostpath-test-pvc
    EOF
```
## 📊 第七步：监控和调试

1. 日志查看

**CSI Controller 日志**

kubectl logs -n hostpath-csi-system -l app=hostpath-csi-controller -c hostpath-csi-controller

**CSI Node 日志**

kubectl logs -n hostpath-csi-system -l app=hostpath-csi-driver -c hostpath-csi-driver

**Provisioner 日志**

kubectl logs -n hostpath-csi-system -l app=hostpath-csi-controller -c csi-provisioner

**Node Driver Registrar 日志**

kubectl logs -n hostpath-csi-system -l app=hostpath-csi-driver -c node-driver-registrar


2. 状态检查

**检查 CSI 驱动注册**

kubectl get csinodes -o yaml

**检查卷状态**

kubectl get pv,pvc

**检查事件**

kubectl get events --sort-by=.metadata.creationTimestamp

**检查节点上的实际目录**

minikube ssh "sudo ls -la /var/lib/hostpath-csi/"


3. 故障排除脚本
#!/bin/bash

**debug.sh - CSI 驱动调试脚本**
```
echo "=== CSI Driver Debug Information ==="

echo "1. Checking CSI Pods..."
kubectl get pods -n hostpath-csi-system

echo -e "\n2. Checking CSI Driver Registration..."
kubectl get csidriver hostpath.csi.example.com

echo -e "\n3. Checking CSI Nodes..."
kubectl get csinodes

echo -e "\n4. Checking Storage Classes..."
kubectl get storageclass hostpath-csi

echo -e "\n5. Checking PVCs..."
kubectl get pvc

echo -e "\n6. Checking PVs..."
kubectl get pv

echo -e "\n7. Recent Events..."
kubectl get events --sort-by=.metadata.creationTimestamp | tail -10

echo -e "\n8. CSI Controller Logs (last 20 lines)..."
kubectl logs -n hostpath-csi-system -l app=hostpath-csi-controller -c hostpath-csi-controller --tail=20

echo -e "\n9. CSI Node Logs (last 20 lines)..."
kubectl logs -n hostpath-csi-system -l app=hostpath-csi-driver -c hostpath-csi-driver --tail=20

echo -e "\n=== Debug Complete ==="
```
## ⚠️ 重要注意事项

1. 🔒 安全考虑
权限管理

CSI 驱动需要 privileged 权限才能执行挂载操作
确保 RBAC 权限最小化，只授予必要的权限
生产环境中应使用专用的 ServiceAccount
数据安全

**建议的安全配置**

```
securityContext:
  privileged: true
  capabilities:
    add: ["SYS_ADMIN"]
  allowPrivilegeEscalation: true
```



2. 📈 性能优化
资源限制

```
resources:
  limits:
    cpu: 200m
    memory: 200Mi
  requests:
    cpu: 100m
    memory: 100Mi
```

存储性能

HostPath 性能接近本地文件系统
避免在网络存储上创建 HostPath 卷
考虑使用 SSD 存储提升性能
3. 🔄 高可用性
Controller 高可用

```
spec:
  replicas: 3  # 多副本部署
  strategy:
    type: RollingUpdate
```

数据备份

**定期备份脚本**

#!/bin/bash
BACKUP_DIR="/backup/hostpath-csi/$(date +%Y%m%d)"
mkdir -p "$BACKUP_DIR"
cp -r /var/lib/hostpath-csi/* "$BACKUP_DIR/"

通过本文的完整实现，您已经掌握了：

## **✅ 技术成果**

- 完整的 CSI 驱动实现 - 支持动态卷供应和管理
- 生产级代码质量 - 包含错误处理、日志记录、资源管理
- 容器化部署 - Docker 镜像构建和 Kubernetes 部署
- 完整的测试验证 - 功能测试、性能测试、故障测试

## **🔧 关键技术点**

- CSI 接口实现 - Identity、Controller、Node 三大服务
- gRPC 服务开发 - 基于 Protobuf 的高性能通信
- Linux 挂载技术 - bind mount 实现目录映射
- Kubernetes 集成 - RBAC、DaemonSet、Deployment 等
- 这个 HostPath CSI 驱动程序为您提供了一个完整的存储插件开发模板，可以根据实际需求进行定制和扩展。

## 参考文档

https://kubernetes-csi.github.io/docs/

https://github.com/container-storage-interface/spec?tab=readme-ov-file