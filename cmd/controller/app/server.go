/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package app

import (
	"context"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	"sigs.k8s.io/scheduler-plugins/pkg/controller"
	pgclientset "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned"
	pgformers "sigs.k8s.io/scheduler-plugins/pkg/generated/informers/externalversions"
)

func newConfig(kubeconfig, master string, inCluster bool) (*restclient.Config, error) {
	var (
		config *rest.Config
		err    error
	)
	if inCluster {
		config, err = rest.InClusterConfig()
	} else {
		config, err = clientcmd.BuildConfigFromFlags(master, kubeconfig)
	}
	if err != nil {
		return nil, err
	}
	config.Burst = 200
	config.QPS = 100
	return config, nil
}

func Run(s *ServerRunOptions) error {
	ctx := context.Background()
	config, err := newConfig(s.KubeConfig, s.MasterUrl, s.InCluster)
	if err != nil {
		klog.Fatal(err)
	}

	stopCh := server.SetupSignalHandler()

	pgClient := pgclientset.NewForConfigOrDie(config)
	kubeClient := kubernetes.NewForConfigOrDie(config)

	scheduleInformer := pgformers.NewSharedInformerFactory(pgClient, 0)
	pgInformer := scheduleInformer.Scheduling().V1alpha1().PodGroups()

	informerFactory := informers.NewSharedInformerFactoryWithOptions(kubeClient, 0, informers.WithTweakListOptions(func(opt *metav1.ListOptions) {
		opt.LabelSelector = controller.PodGroupLabel
	}))
	podInformer := informerFactory.Core().V1().Pods()
	ctrl := controller.NewPodGroupController(kubeClient, pgInformer, podInformer, pgClient)
	informerFactory.Start(ctx.Done())
	scheduleInformer.Start(ctx.Done())
	run := func(ctx context.Context) {
		ctrl.Run(10, ctx.Done())
	}

	if !s.EnableLeaderElection {
		run(ctx)

	} else {
		id, err := os.Hostname()
		if err != nil {
			return err
		}

		// add a uniquifier so that two processes on the same host don't accidentally both become active
		id = id + "_" + string(uuid.NewUUID())

		rl, err := resourcelock.New("endpoints",
			"kube-system",
			"kube-scheduler",
			kubeClient.CoreV1(),
			kubeClient.CoordinationV1(),
			resourcelock.ResourceLockConfig{
				Identity: id,
			})
		if err != nil {
			klog.Fatalf("error creating lock: %v", err)
		}

		leaderelection.RunOrDie(context.TODO(), leaderelection.LeaderElectionConfig{
			Lock: rl,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: run,
				OnStoppedLeading: func() {
					klog.Fatalf("leaderelection lost")
				},
			},
			Name: "podgroup controller",
		})
	}

	<-stopCh
	return nil
}
