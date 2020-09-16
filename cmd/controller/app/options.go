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
	"github.com/spf13/pflag"
)

type ServerRunOptions struct {
	KubeConfig           string
	MasterUrl            string
	InCluster            bool
	ApiServerQPS         uint32
	ApiServerBurst       uint32
	EnableLeaderElection bool
}

func NewServerRunOptions() *ServerRunOptions {
	options := &ServerRunOptions{}
	options.addAllFlags()
	return options
}

func (s *ServerRunOptions) addAllFlags() {
	pflag.BoolVar(&s.InCluster, "incluster", s.InCluster, "If controller run incluster.")
	pflag.StringVar(&s.MasterUrl, "kubeConfig", s.MasterUrl, "Kube Config path if not run in cluster.")
	pflag.StringVar(&s.MasterUrl, "masterUrl", s.MasterUrl, "Master Url if not run in cluster.")
	pflag.Uint32Var(&s.ApiServerBurst, "qps", 5, "qps of query apiserver.")
	pflag.Uint32Var(&s.ApiServerQPS, "burst", 10, "burst of query apiserver.")
	pflag.BoolVar(&s.EnableLeaderElection, "enableLeaderElection", s.EnableLeaderElection, "If EnableLeaderElection for controller.")
}
