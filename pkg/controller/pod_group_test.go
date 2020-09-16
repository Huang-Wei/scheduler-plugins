package controller

import (
	"context"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/controller"
	st "k8s.io/kubernetes/pkg/scheduler/testing"

	"sigs.k8s.io/scheduler-plugins/pkg/apis/podgroup/v1alpha1"
	pgfake "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned/fake"
	pgformers "sigs.k8s.io/scheduler-plugins/pkg/generated/informers/externalversions"
)

func Test_Run(t *testing.T) {
	ctx := context.TODO()

	cases := []struct {
		name              string
		pgName            string
		minMember         uint32
		podNames          []string
		podPhase          v1.PodPhase
		previousPhase     v1alpha1.PodGroupPhase
		desiredGroupPhase v1alpha1.PodGroupPhase
	}{
		{
			name:              "Group running",
			pgName:            "pg1",
			minMember:         2,
			podNames:          []string{"pod1", "pod2"},
			podPhase:          v1.PodRunning,
			previousPhase:     v1alpha1.PodGroupScheduled,
			desiredGroupPhase: v1alpha1.PodGroupRunning,
		},
		{
			name:              "Group running, more than min member",
			pgName:            "pg11",
			minMember:         2,
			podNames:          []string{"pod11", "pod21"},
			podPhase:          v1.PodRunning,
			previousPhase:     v1alpha1.PodGroupScheduled,
			desiredGroupPhase: v1alpha1.PodGroupRunning,
		},
		{
			name:              "Group failed",
			pgName:            "pg2",
			minMember:         2,
			podNames:          []string{"pod1", "pod2"},
			podPhase:          v1.PodFailed,
			previousPhase:     v1alpha1.PodGroupScheduled,
			desiredGroupPhase: v1alpha1.PodGroupFailed,
		},
		{
			name:              "Group finished",
			pgName:            "pg3",
			minMember:         2,
			podNames:          []string{"pod1", "pod2"},
			podPhase:          v1.PodSucceeded,
			previousPhase:     v1alpha1.PodGroupScheduled,
			desiredGroupPhase: v1alpha1.PodGroupFinished,
		},
		{
			name:              "Group status convert from scheduling to scheduled",
			pgName:            "pg4",
			minMember:         2,
			podNames:          []string{"pod1", "pod2"},
			podPhase:          v1.PodPending,
			previousPhase:     v1alpha1.PodGroupScheduling,
			desiredGroupPhase: v1alpha1.PodGroupScheduled,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ps := pods([]string{"pod1", "pod2"}, c.pgName, c.podPhase)
			kubeClient := fake.NewSimpleClientset(ps[0], ps[1])
			pg := pg(c.pgName, 2, c.previousPhase)
			pgClient := pgfake.NewSimpleClientset(pg)

			informerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())
			schedulingInformerFactory := pgformers.NewSharedInformerFactory(pgClient, controller.NoResyncPeriodFunc())
			podInformer := informerFactory.Core().V1().Pods()
			pgInformer := schedulingInformerFactory.Scheduling().V1alpha1().PodGroups()
			ctrl := NewPodGroupController(kubeClient, pgInformer, podInformer, pgClient)

			informerFactory.Start(ctx.Done())
			schedulingInformerFactory.Start(ctx.Done())

			go ctrl.Run(1, ctx.Done())
			time.Sleep(200 * time.Millisecond)

			pg, err := pgClient.SchedulingV1alpha1().PodGroups("default").Get(ctx, c.pgName, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if pg.Status.Phase != c.desiredGroupPhase {
				t.Fatalf("descired %v, get %v", c.desiredGroupPhase, pg.Status.Phase)
			}
		})
	}

}

func pods(podNames []string, pgName string, phase v1.PodPhase) []*v1.Pod {
	pds := make([]*v1.Pod, 0)
	for _, name := range podNames {
		pod := st.MakePod().Namespace("default").Name(name).Obj()
		pod.Labels = map[string]string{PodGroupLabel: pgName}
		pod.Status.Phase = phase
		pds = append(pds, pod)
	}
	return pds
}

func pg(pgName string, minMember uint32, previousPhase v1alpha1.PodGroupPhase) *v1alpha1.PodGroup {
	return &v1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              pgName,
			Namespace:         "default",
			CreationTimestamp: metav1.Time{time.Now()},
		},
		Spec: v1alpha1.PodGroupSpec{
			MinMember: minMember,
		},
		Status: v1alpha1.PodGroupStatus{
			OccupiedBy:        "test",
			Scheduled:         minMember,
			ScheduleStartTime: metav1.Time{time.Now()},
			Phase:             previousPhase,
		},
	}
}
