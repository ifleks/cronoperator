/*
Copyright 2026.

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
	"maps"
	"slices"
	"time"

	"github.com/robfig/cron"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kbatch "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/api/meta"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ref "k8s.io/client-go/tools/reference"

	batchv1 "github.com/ifleks/cronoperator/api/v1"
)

type realClock struct{}

func (_ realClock) Now() time.Time {
	return time.Now()
}

type Clock interface {
	Now() time.Time
}

var (
	jobOwnerKey = ".metadata.controller"
	apiGVStr    = batchv1.GroupVersion.String()
)

const (
	// typeAvailableCronJob represents the status of the CronJob reconciliation
	typeAvailableCronJob = "Available"
	// typeProgressingCronJob represents the status used when the CronJob is being reconciled
	typeProgressingCronJob = "Progressing"
	// typeDegradedCronJob represents the status used when the CronJob has encountered an error
	typeDegradedCronJob = "Degraded"
)

var (
	scheduledTimeAnnotation = "batch.tutorial.w-s-g.ru/scheduled-at"
)

// CronJobReconciler reconciles a CronJob object
type CronJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Clock
}

// +kubebuilder:rbac:groups=batch.tutorial.w-s-g.ru,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch.tutorial.w-s-g.ru,resources=cronjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch.tutorial.w-s-g.ru,resources=cronjobs/finalizers,verbs=update

// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the CronJob object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/reconcile
func (r *CronJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var cronJob batchv1.CronJob
	if err := r.Get(ctx, req.NamespacedName, &cronJob); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("CronJob resource not found. Ignoring since object must be deleted.")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get CronJob")
		return ctrl.Result{}, err
	}

	if len(cronJob.Status.Conditions) == 0 {
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeProgressingCronJob,
			Status:  metav1.ConditionUnknown,
			Reason:  "Reconciling",
			Message: "Starting reconciliation",
		})
		if err := r.Status().Update(ctx, &cronJob); err != nil {
			log.Error(err, "Failed to update CronJob status")
			return ctrl.Result{}, err
		}
	}

	if err := r.Get(ctx, req.NamespacedName, &cronJob); err != nil {
		log.Error(err, "Failed to get CronJob after status update")
		return ctrl.Result{}, err
	}

	var childJobs kbatch.JobList

	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), client.MatchingFields{jobOwnerKey: req.Name}); err != nil {
		log.Error(err, "Failed to list child Jobs", "cronjob", req.Name)

		if fetchErr := r.Get(ctx, req.NamespacedName, &cronJob); fetchErr != nil {
			log.Error(fetchErr, "Failed to get CronJob for status update after listing child Jobs")
			return ctrl.Result{}, fetchErr
		}

		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeDegradedCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "ErrorListingChildJobs",
			Message: "Failed to list child Jobs",
		})

		if err := r.Status().Update(ctx, &cronJob); err != nil {
			log.Error(err, "Failed to update CronJob status after listing child Jobs error")
		}

		return ctrl.Result{}, err
	}

	var activeJobs []*kbatch.Job
	var successfulJobs []*kbatch.Job
	var failedJobs []*kbatch.Job
	var mostRecentTime *time.Time

	for i, job := range childJobs.Items {
		_, finishedType := isJobFinished(&job)
		switch finishedType {
		case "": // ongoing
			activeJobs = append(activeJobs, &childJobs.Items[i])
		case kbatch.JobFailed:
			failedJobs = append(failedJobs, &childJobs.Items[i])
		case kbatch.JobComplete:
			successfulJobs = append(successfulJobs, &childJobs.Items[i])
		}

		scheduledTimeForJob, err := getScheduledTimeForJob(&job)
		if err != nil {
			log.Error(err, "Failed to parse scheduled time for Job", "job", job.Name)
			continue
		}
		if scheduledTimeForJob != nil {
			if mostRecentTime == nil || scheduledTimeForJob.After(*mostRecentTime) {
				mostRecentTime = scheduledTimeForJob
			}
		}
	}

	if mostRecentTime != nil {
		cronJob.Status.LastScheduleTime = &metav1.Time{Time: *mostRecentTime}
	} else {
		cronJob.Status.LastScheduleTime = nil
	}

	cronJob.Status.Active = nil

	for _, activeJob := range activeJobs {
		jobRef, err := ref.GetReference(r.Scheme, activeJob)
		if err != nil {
			log.Error(err, "Failed to get reference for active Job", "job", activeJob.Name)
			continue
		}
		cronJob.Status.Active = append(cronJob.Status.Active, *jobRef)
	}

	isSuspended := cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend
	if isSuspended {
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeAvailableCronJob,
			Status:  metav1.ConditionFalse,
			Reason:  "Suspended",
			Message: "CronJob is suspended",
		})
	} else if len(failedJobs) > 0 {
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeDegradedCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "JobsFailed",
			Message: fmt.Sprintf("%d job(s) have failed", len(failedJobs)),
		})
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeAvailableCronJob,
			Status:  metav1.ConditionFalse,
			Reason:  "JobsFailed",
			Message: fmt.Sprintf("%d job(s) have failed", len(failedJobs)),
		})
	} else if len(activeJobs) > 0 {
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeProgressingCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "JobsActive",
			Message: fmt.Sprintf("%d job(s) are currently active", len(activeJobs)),
		})
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeAvailableCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "JobsActive",
			Message: fmt.Sprintf("CronJob is progressing with %d active job(s)", len(activeJobs)),
		})
	} else {
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeAvailableCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "AllJobsCompleted",
			Message: "All jobs have completed successfully",
		})
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeProgressingCronJob,
			Status:  metav1.ConditionFalse,
			Reason:  "NoJobsActive",
			Message: "No jobs are currently active",
		})
	}

	if err := r.Status().Update(ctx, &cronJob); err != nil {
		log.Error(err, "unable to update CronJob status")
		return ctrl.Result{}, err
	}

	if cronJob.Spec.FailedJobsHistoryLimit != nil {
		slices.SortStableFunc(failedJobs, func(a, b *kbatch.Job) int {
			aStartTime := a.Status.StartTime
			bStartTime := b.Status.StartTime
			if aStartTime == nil && bStartTime != nil {
				return 1
			}

			if aStartTime.Before(bStartTime) {
				return -1
			} else if bStartTime.Before(aStartTime) {
				return 1
			}
			return 0
		})
		for i, job := range failedJobs {
			if int32(i) >= int32(len(failedJobs))-*cronJob.Spec.FailedJobsHistoryLimit {
				break
			}
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				log.Error(err, "unable to delete old failed job", "job", job)
			} else {
				log.V(1).Info("deleted old failed job", "job", job)
			}
		}
	}

	if cronJob.Spec.SuccessfulJobsHistoryLimit != nil {
		slices.SortStableFunc(successfulJobs, func(a, b *kbatch.Job) int {
			aStartTime := a.Status.StartTime
			bStartTime := b.Status.StartTime
			if aStartTime == nil && bStartTime != nil {
				return 1
			}

			if aStartTime.Before(bStartTime) {
				return -1
			} else if bStartTime.Before(aStartTime) {
				return 1
			}
			return 0
		})
		for i, job := range successfulJobs {
			if int32(i) >= int32(len(successfulJobs))-*cronJob.Spec.SuccessfulJobsHistoryLimit {
				break
			}
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
				log.Error(err, "unable to delete old successful job", "job", job)
			} else {
				log.V(1).Info("deleted old successful job", "job", job)
			}
		}
	}

	if cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend {
		log.V(1).Info("cronjob suspended, skipping")
		return ctrl.Result{}, nil
	}
	getNextSchedule := func(cronJob *batchv1.CronJob, now time.Time) (lastMissed time.Time, next time.Time, err error) {
		sched, err := cron.ParseStandard(cronJob.Spec.Schedule)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("unparseable schedule %q: %w", cronJob.Spec.Schedule, err)
		}

		// for optimization purposes, cheat a bit and start from our last observed run time
		// we could reconstitute this here, but there's not much point, since we've
		// just updated it.
		var earliestTime time.Time
		if cronJob.Status.LastScheduleTime != nil {
			earliestTime = cronJob.Status.LastScheduleTime.Time
		} else {
			earliestTime = cronJob.CreationTimestamp.Time
		}
		if cronJob.Spec.StartingDeadlineSeconds != nil {
			// controller is not going to schedule anything below this point
			schedulingDeadline := now.Add(-time.Second * time.Duration(*cronJob.Spec.StartingDeadlineSeconds))

			if schedulingDeadline.After(earliestTime) {
				earliestTime = schedulingDeadline
			}
		}
		if earliestTime.After(now) {
			return time.Time{}, sched.Next(now), nil
		}

		starts := 0
		for t := sched.Next(earliestTime); !t.After(now); t = sched.Next(t) {
			lastMissed = t
			// An object might miss several starts. For example, if
			// controller gets wedged on Friday at 5:01pm when everyone has
			// gone home, and someone comes in on Tuesday AM and discovers
			// the problem and restarts the controller, then all the hourly
			// jobs, more than 80 of them for one hourly scheduledJob, should
			// all start running with no further intervention (if the scheduledJob
			// allows concurrency and late starts).
			//
			// However, if there is a bug somewhere, or incorrect clock
			// on controller's server or apiservers (for setting creationTimestamp)
			// then there could be so many missed start times (it could be off
			// by decades or more), that it would eat up all the CPU and memory
			// of this controller. In that case, we want to not try to list
			// all the missed start times.
			starts++
			if starts > 100 {
				// We can't get the most recent times so just return an empty slice
				return time.Time{}, time.Time{}, fmt.Errorf("Too many missed start times (> 100). Set or decrease .spec.startingDeadlineSeconds or check clock skew.") //nolint:staticcheck
			}
		}
		return lastMissed, sched.Next(now), nil
	}

	missedRun, nextRun, err := getNextSchedule(&cronJob, r.Now())
	if err != nil {
		log.Error(err, "unable to figure out CronJob schedule")
		if fetchErr := r.Get(ctx, req.NamespacedName, &cronJob); fetchErr != nil {
			log.Error(fetchErr, "Failed to re-fetch CronJob")
			return ctrl.Result{}, fetchErr
		}
		// Update status condition to reflect the schedule error
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeDegradedCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "InvalidSchedule",
			Message: fmt.Sprintf("Failed to parse schedule: %v", err),
		})
		if statusErr := r.Status().Update(ctx, &cronJob); statusErr != nil {
			log.Error(statusErr, "Failed to update CronJob status")
		}
		// we don't really care about requeuing until we get an update that
		// fixes the schedule, so don't return an error
		return ctrl.Result{}, nil
	}

	scheduledResult := ctrl.Result{RequeueAfter: nextRun.Sub(r.Now())} // save this so we can re-use it elsewhere
	log = log.WithValues("now", r.Now(), "next run", nextRun)

	if missedRun.IsZero() {
		log.V(1).Info("no upcoming scheduled times, sleeping until next")
		return scheduledResult, nil
	}

	// make sure we're not too late to start the run
	log = log.WithValues("current run", missedRun)
	tooLate := false
	if cronJob.Spec.StartingDeadlineSeconds != nil {
		tooLate = missedRun.Add(time.Duration(*cronJob.Spec.StartingDeadlineSeconds) * time.Second).Before(r.Now())
	}
	if tooLate {
		log.V(1).Info("missed starting deadline for last run, sleeping till next")
		if fetchErr := r.Get(ctx, req.NamespacedName, &cronJob); fetchErr != nil {
			log.Error(fetchErr, "Failed to re-fetch CronJob")
			return ctrl.Result{}, fetchErr
		}
		// Update status condition to reflect missed deadline
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeDegradedCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "MissedSchedule",
			Message: fmt.Sprintf("Missed starting deadline for run at %v", missedRun),
		})
		if statusErr := r.Status().Update(ctx, &cronJob); statusErr != nil {
			log.Error(statusErr, "Failed to update CronJob status")
		}
		return scheduledResult, nil
	}

	if cronJob.Spec.ConcurrencyPolicy == batchv1.ForbidConcurrent && len(activeJobs) > 0 {
		log.V(1).Info("concurrency policy blocks concurrent runs, skipping", "num active", len(activeJobs))
		return scheduledResult, nil
	}

	// ...or instruct us to replace existing ones...
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ReplaceConcurrent {
		for _, activeJob := range activeJobs {
			// we don't care if the job was already deleted
			if err := r.Delete(ctx, activeJob, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				log.Error(err, "unable to delete active job", "job", activeJob)
				return ctrl.Result{}, err
			}
		}
	}

	constructJobForCronJob := func(cronJob *batchv1.CronJob, scheduledTime time.Time) (*kbatch.Job, error) {
		// We want job names for a given nominal start time to have a deterministic name to avoid the same job being created twice
		name := fmt.Sprintf("%s-%d", cronJob.Name, scheduledTime.Unix())

		job := &kbatch.Job{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      make(map[string]string),
				Annotations: make(map[string]string),
				Name:        name,
				Namespace:   cronJob.Namespace,
			},
			Spec: *cronJob.Spec.JobTemplate.Spec.DeepCopy(),
		}

		maps.Copy(job.Annotations, cronJob.Spec.JobTemplate.Annotations)
		job.Annotations[scheduledTimeAnnotation] = scheduledTime.Format(time.RFC3339)
		maps.Copy(job.Labels, cronJob.Spec.JobTemplate.Labels)
		if err := ctrl.SetControllerReference(cronJob, job, r.Scheme); err != nil {
			return nil, err
		}

		return job, nil
	}

	// actually make the job...
	job, err := constructJobForCronJob(&cronJob, missedRun)
	if err != nil {
		log.Error(err, "unable to construct job from template")
		// don't bother requeuing until we get a change to the spec
		return scheduledResult, nil
	}

	// ...and create it on the cluster
	if err := r.Create(ctx, job); err != nil {
		log.Error(err, "unable to create Job for CronJob", "job", job)
		if fetchErr := r.Get(ctx, req.NamespacedName, &cronJob); fetchErr != nil {
			log.Error(fetchErr, "Failed to re-fetch CronJob")
			return ctrl.Result{}, fetchErr
		}
		// Update status condition to reflect the error
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeDegradedCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "JobCreationFailed",
			Message: fmt.Sprintf("Failed to create job: %v", err),
		})
		if statusErr := r.Status().Update(ctx, &cronJob); statusErr != nil {
			log.Error(statusErr, "Failed to update CronJob status")
		}
		return ctrl.Result{}, err
	}

	log.V(1).Info("created Job for CronJob run", "job", job)

	if fetchErr := r.Get(ctx, req.NamespacedName, &cronJob); fetchErr != nil {
		log.Error(fetchErr, "Failed to re-fetch CronJob")
		return ctrl.Result{}, fetchErr
	}

	// Update status condition to reflect successful job creation
	meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
		Type:    typeProgressingCronJob,
		Status:  metav1.ConditionTrue,
		Reason:  "JobCreated",
		Message: fmt.Sprintf("Created job %s", job.Name),
	})

	if statusErr := r.Status().Update(ctx, &cronJob); statusErr != nil {
		log.Error(statusErr, "Failed to update CronJob status")
	}

	return scheduledResult, nil
}

func isJobFinished(job *kbatch.Job) (bool, kbatch.JobConditionType) {
	for _, c := range job.Status.Conditions {
		if (c.Type == kbatch.JobComplete || c.Type == kbatch.JobFailed) && c.Status == corev1.ConditionTrue {
			return true, c.Type
		}
	}
	return false, ""
}

func getScheduledTimeForJob(job *kbatch.Job) (*time.Time, error) {
	timeRaw, ok := job.Annotations[scheduledTimeAnnotation]
	if len(timeRaw) == 0 || !ok {
		return nil, nil
	}
	timeParsed, err := time.Parse(time.RFC3339, timeRaw)
	if err != nil {
		return nil, err
	}
	return &timeParsed, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {

	if r.Clock == nil {
		r.Clock = realClock{}
	}

	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &kbatch.Job{}, jobOwnerKey, func(rawObj client.Object) []string {
		job := rawObj.(*kbatch.Job)
		owner := metav1.GetControllerOf(job)
		if owner == nil {
			return nil
		}
		if owner.APIVersion != apiGVStr || owner.Kind != "CronJob" {
			return nil
		}
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.CronJob{}).
		Owns(&kbatch.Job{}).
		Named("cronjob").
		Complete(r)
}
