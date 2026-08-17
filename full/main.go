package main

import (
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/graphene-ci/contrib/docker/pkg/docker"
	"github.com/graphene-ci/gctl/pkg/graphene"
	"github.com/graphene-ci/gctl/pkg/kernel"
)

type Params struct {
	// Куда ставить машину.
	Zone     string `json:"zone"`
	Subnet   string `json:"subnet"`
	Image    string `json:"image"`
	Cores    int    `json:"cores"`
	MemoryGB int    `json:"memoryGB"`

	// Чей кластер держит провайдера. Пусто — обычные правила, то есть
	// kubeconfig того, кто поднял воркер.
	Kubeconfig string `json:"kubeconfig"`
	Namespace  string `json:"namespace"`

	// Work is the command whose output becomes the report. A perf test in
	// real life; anything at all here.
	Work string `json:"work"`

	// Keep leaves the machine standing after the run instead of removing
	// it — for the morning when somebody wants to look at what failed.
	Keep time.Duration `json:"keep"`
}

func main() {
	graphene.Pipeline(kernel.Control("0.0.0.0"), func(ctx graphene.Context, pipe graphene.PipelineRun, params Params) error {

		uniqId := pipe.RunId()
		k8sConfig := ctx.GetFromEnv("K8S_CONFIG")

		k8sClient := k8slib.NewClient(ctx, k8sConfig)

		yc := k8sClient.Resource(yandex.Provider(ysdk.ProviderArgs{
			FolderId: sdk.String("b1gxxxxxxxxxxxxxxxxx"),
			Zone:     sdk.String("ru-central1-a"),
			Token:    sdk.String(token.Path()),
		}))

		net := k8sClient.Resource(yandex.VpcNetwork("net", ysdk.VpcNetworkArgs{}))
		// hidden wait for net

		sub := k8sClient.Resource(yandex.VpcSubnet("sub", ysdk.VpcSubnetArgs{
			NetworkId:    net.Ready().Id(),
			Zone:         sdk.String("ru-central1-a"),
			V4CidrBlocks: sdk.StringArray{sdk.String("10.0.0.0/24")},
		}))
		// hidden wait for sub

		vmAgent := kernel.NewAgent("edge-1") // may be agent == system resource with internal controller

		vm := k8slib.Resource(yandex.ComputeInstance("vm-1", ysdk.ComputeInstanceArgs{
			Zone: sdk.String("ru-central1-a"),
			Resources: ysdk.ComputeInstanceResourcesArgs{
				Cores:  sdk.Int(2),
				Memory: sdk.Float64(4),
			},
			BootDisk: ysdk.ComputeInstanceBootDiskArgs{
				InitializeParams: ysdk.ComputeInstanceBootDiskInitializeParamsArgs{
					ImageId: sdk.String("fd8kdq6d0p8sij7h5qe3"),
					Size:    sdk.Int(20),
				},
			},
			NetworkInterfaces: ysdk.ComputeInstanceNetworkInterfaceArray{
				ysdk.ComputeInstanceNetworkInterfaceArgs{
					SubnetId: sub.Ready().Id(),
					Nat:      sdk.Bool(true),
				},
			},
			Metadata: sdk.StringMap{
				// Installs graphened and tells it where to call. No
				// secret in it: the machine proves who it is with what
				// it IS — the identity its cloud gives it.
				"user-data": sdk.String(vmAgent.CloudInit()),
			},
		}))
		// hidden wait for vm

		pipe.WaitAgentReady(vmAgent)

		// this our library for docker manipulating over agent api
		dockerLib.Install(vmAgent)
		// dockerLib may create Container resource - resource like
		// k8slib.Resource but hidden from user pipeline (visible in client|ui)
		//for sugar syntax and living in vmAgent and die with him
		dockerLib.Container(vmAgent, "hello", docker.Spec{
			Config: &container.Config{
				Image:        "nginx:alpine",
				ExposedPorts: nat.PortSet{"80/tcp": {}},
			},
			Host: &container.HostConfig{
				RestartPolicy: container.RestartPolicy{Name: "always"},
			},
		})

		return nil
	})
}
