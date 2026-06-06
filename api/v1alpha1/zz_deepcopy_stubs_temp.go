// Temporary stubs - will be replaced by generated zz_generated.deepcopy.go in Task 6.
package v1alpha1

import "k8s.io/apimachinery/pkg/runtime"

func (in *Project) DeepCopyObject() runtime.Object     { return in.DeepCopy() }
func (in *Project) DeepCopy() *Project                  { out := *in; return &out }
func (in *ProjectList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
func (in *ProjectList) DeepCopy() *ProjectList          { out := *in; return &out }

func (in *Repository) DeepCopyObject() runtime.Object     { return in.DeepCopy() }
func (in *Repository) DeepCopy() *Repository               { out := *in; return &out }
func (in *RepositoryList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
func (in *RepositoryList) DeepCopy() *RepositoryList       { out := *in; return &out }

func (in *Task) DeepCopyObject() runtime.Object     { return in.DeepCopy() }
func (in *Task) DeepCopy() *Task                     { out := *in; return &out }
func (in *TaskList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
func (in *TaskList) DeepCopy() *TaskList             { out := *in; return &out }

func (in *Subtask) DeepCopyObject() runtime.Object     { return in.DeepCopy() }
func (in *Subtask) DeepCopy() *Subtask                  { out := *in; return &out }
func (in *SubtaskList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
func (in *SubtaskList) DeepCopy() *SubtaskList          { out := *in; return &out }
