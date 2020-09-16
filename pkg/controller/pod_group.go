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

package controller

import (
	"context"
	"fmt"
	"reflect"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformer "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelister "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	pgv1 "sigs.k8s.io/scheduler-plugins/pkg/apis/podgroup/v1alpha1"
	pgclientset "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned"
	pginformer "sigs.k8s.io/scheduler-plugins/pkg/generated/informers/externalversions/podgroup/v1alpha1"
	pglister "sigs.k8s.io/scheduler-plugins/pkg/generated/listers/podgroup/v1alpha1"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
)

const (
	// PodGroupLabel is the default label of batch scheduler
	PodGroupLabel = "podgroup.scheduling.sigs.k8s.io"
)

var (
	// ErrorNotMatched means pod does not match batch scheduling
	ErrorNotMatched = fmt.Errorf("not match batch scheduling")
	// ErrorWaiting means pod number does not match the min pods required
	ErrorWaiting = fmt.Errorf("waiting")
	// ErrorResourceNotEnough means cluster resource is not enough, mainly used in Pre-Filter
	ErrorResourceNotEnough = fmt.Errorf("resource not enough")
)

// PodGroupController is a controller that process pod groups using provided Handler interface
type PodGroupController struct {
	eventRecorder   record.EventRecorder
	pgQueue         workqueue.RateLimitingInterface
	pgLister        pglister.PodGroupLister
	podLister       corelister.PodLister
	pgListerSynced  cache.InformerSynced
	podListerSynced cache.InformerSynced
	pgClient        pgclientset.Interface
}

// NewPodGroupController returns a new *PodGroupController
func NewPodGroupController(client kubernetes.Interface,
	pgInformer pginformer.PodGroupInformer,
	podInformer coreinformer.PodInformer,
	pgClient pgclientset.Interface) *PodGroupController {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: client.CoreV1().Events(v1.NamespaceAll)})
	var eventRecorder record.EventRecorder
	eventRecorder = broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "Coscheduling"})

	ctrl := &PodGroupController{
		eventRecorder: eventRecorder,
		pgQueue:       workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Coscheduling-queue"),
	}

	pgInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.pgAdded,
		UpdateFunc: ctrl.pgUpdated,
	})

	ctrl.pgLister = pgInformer.Lister()
	ctrl.podLister = podInformer.Lister()
	ctrl.pgListerSynced = pgInformer.Informer().HasSynced
	ctrl.podListerSynced = podInformer.Informer().HasSynced
	ctrl.pgClient = pgClient
	return ctrl
}

// Run starts listening on channel events
func (ctrl *PodGroupController) Run(workers int, stopCh <-chan struct{}) {
	defer ctrl.pgQueue.ShutDown()

	klog.Info("Starting coscheduling")
	defer klog.Info("Shutting coscheduling")

	if !cache.WaitForCacheSync(stopCh, ctrl.pgListerSynced, ctrl.pgListerSynced) {
		klog.Error("Cannot sync caches")
		return
	}
	klog.Info("Coscheduling sync finished")
	for i := 0; i < workers; i++ {
		go wait.Until(ctrl.sync, 0, stopCh)
	}

	<-stopCh
}

// pgAdded reacts to a PG creation
func (ctrl *PodGroupController) pgAdded(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	pg := obj.(*pgv1.PodGroup)
	if pg.Status.Phase == pgv1.PodGroupFinished || pg.Status.Phase == pgv1.PodGroupFailed {
		return
	}
	// If startScheduleTime - createTime > 2days, do not enqueue again because pod may have be GC
	if pg.Status.Scheduled == pg.Spec.MinMember && pg.Status.Running == 0 &&
		pg.Status.ScheduleStartTime.Sub(pg.CreationTimestamp.Time) > 48*time.Hour {
		return
	}
	klog.Info("enqueue ", "key ", key)
	ctrl.pgQueue.Add(key)
}

// pgUpdated reacts to a PG update
func (ctrl *PodGroupController) pgUpdated(old, new interface{}) {
	ctrl.pgAdded(new)
}

// pgDelete reacts to a PG update
func (ctrl *PodGroupController) pgDelete(new interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(new)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	klog.Info("enqueue ", "key ", key)
	klog.V(3).Infof("pg %q delete change", key)
}

// syncPG deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *PodGroupController) sync() {
	keyObj, quit := ctrl.pgQueue.Get()
	if quit {
		return
	}
	defer ctrl.pgQueue.Done(keyObj)

	key := keyObj.(string)
	namespace, pgName, err := cache.SplitMetaNamespaceKey(key)
	klog.V(4).Infof("Started PG processing %q", pgName)

	// get PG to process
	pg, err := ctrl.pgLister.PodGroups(namespace).Get(pgName)
	ctx := context.TODO()
	if err != nil {
		if apierrs.IsNotFound(err) {
			pg, err = ctrl.pgClient.SchedulingV1alpha1().PodGroups(namespace).Get(ctx, pgName, metav1.GetOptions{})
			if err != nil && apierrs.IsNotFound(err) {
				// PG was deleted in the meantime, ignore.
				klog.V(3).Infof("PG %q deleted", pgName)
				return
			}
		}
		klog.Errorf("Error getting PodGroup %q: %v", pgName, err)
		ctrl.pgQueue.AddRateLimited(keyObj)
		return
	}
	ctrl.syncHandler(ctx, pg)
}

// syncHandle syncs pod group and convert status
func (ctrl *PodGroupController) syncHandler(ctx context.Context, pg *pgv1.PodGroup) {

	key, err := cache.MetaNamespaceKeyFunc(pg)
	if err != nil {
		runtime.HandleError(err)
		return
	}

	defer func() {
		if err != nil {
			ctrl.pgQueue.AddRateLimited(key)
			return
		}
	}()

	pgCopy := pg.DeepCopy()
	if string(pgCopy.Status.Phase) == "" {
		pgCopy.Status.Phase = pgv1.PodGroupPending
	}

	selector := labels.Set(map[string]string{PodGroupLabel: pgCopy.Name}).AsSelector()
	pods, err := ctrl.podLister.List(selector)
	if err != nil {
		return
	}
	var (
		running   uint32 = 0
		succeeded uint32 = 0
		failed    uint32 = 0
	)
	if len(pods) != 0 {
		for _, pod := range pods {
			switch pod.Status.Phase {
			case v1.PodRunning:
				running++
			case v1.PodSucceeded:
				succeeded++
			case v1.PodFailed:
				failed++
			}
		}
	}
	pgCopy.Status.Failed = failed
	pgCopy.Status.Succeeded = succeeded
	pgCopy.Status.Running = running

	if pgCopy.Status.Scheduled >= pgCopy.Spec.MinMember && pgCopy.Status.Phase == pgv1.PodGroupScheduling {
		pgCopy.Status.Phase = pgv1.PodGroupScheduled
	}

	if pgCopy.Status.Succeeded+pgCopy.Status.Running >= pg.Spec.MinMember && pgCopy.Status.Phase == pgv1.PodGroupScheduled {
		pgCopy.Status.Phase = pgv1.PodGroupRunning
	}
	// Final state of pod group
	if pgCopy.Status.Failed != 0 && pgCopy.Status.Failed+pgCopy.Status.Running+pgCopy.Status.Succeeded >= pg.Spec.
		MinMember {
		pgCopy.Status.Phase = pgv1.PodGroupFailed
	}
	if pgCopy.Status.Succeeded >= pg.Spec.MinMember {
		pgCopy.Status.Phase = pgv1.PodGroupFinished
	}

	err = ctrl.patchPodGroup(pg, pgCopy)
	if err == nil {
		ctrl.pgQueue.Forget(pg)
	}
	if pgCopy.Status.Phase == pgv1.PodGroupFinished || pgCopy.Status.Phase == pgv1.PodGroupFailed {
		return
	}
	ctrl.pgQueue.AddRateLimited(key)
}

func (ctrl *PodGroupController) patchPodGroup(old, new *pgv1.PodGroup) error {
	if !reflect.DeepEqual(old, new) {
		patch, err := util.CreateMergePatch(old, new)
		if err != nil {
			return err
		}

		_, err = ctrl.pgClient.SchedulingV1alpha1().PodGroups(old.Namespace).Patch(context.TODO(), old.Name, types.MergePatchType,
			patch, metav1.PatchOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}
