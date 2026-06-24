package upgrade

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclient "k8s.io/client-go/kubernetes"

	"github.com/longhorn/cli/pkg/types"
	kubeutils "github.com/longhorn/cli/pkg/utils/kubernetes"

	utilslonghorn "github.com/longhorn/cli/pkg/utils/longhorn"
	commonio "github.com/longhorn/go-common-libs/io"
	lhtypes "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhutil "github.com/longhorn/longhorn-manager/util"
)

const (
	DataEngineV1 = "v1"
	DataEngineV2 = "v2"
)

type Checker struct {
	types.GlobalCmdOptions

	DataEngine     string
	OutputFilePath string

	logger         *logrus.Entry
	kubeClient     *kubeclient.Clientset
	longhornClient *utilslonghorn.LonghornClient

	collection *types.LogCollection
}

func (c *Checker) Init() error {
	c.DataEngine = strings.ToLower(strings.TrimSpace(c.DataEngine))
	c.logger = logrus.WithField("data-engine", c.DataEngine)

	kubeClient, err := kubeutils.NewKubeClient("", c.KubeConfigPath)
	if err != nil {
		return errors.Wrap(err, "failed to initialize Kubernetes client")
	}

	client, err := utilslonghorn.NewLonghornClient(c.KubeConfigPath, c.Namespace)
	if err != nil {
		return errors.Wrap(err, "failed to initialize Longhorn client")
	}

	c.kubeClient = kubeClient
	c.longhornClient = client
	c.collection = &types.LogCollection{}

	return nil
}

func (c *Checker) Run() error {
	c.logger.Info("Checking common cluster conditions")
	if err := c.runCommonChecks(); err != nil {
		return err
	}

	switch c.DataEngine {
	case DataEngineV1:
		c.logger.Info("Checking attached v1 volumes")
		if err := c.runV1Checks(); err != nil {
			return err
		}
	case DataEngineV2:
		c.logger.Info("Checking attached v2 volumes")
		if err := c.runV2Checks(); err != nil {
			return err
		}
	default:
		return errors.Errorf("unsupported data engine %q", c.DataEngine)
	}

	if len(c.collection.Error) != 0 {
		return errors.New("upgrade check failed")
	}

	return nil
}

func (c *Checker) runCommonChecks() error {
	if err := c.checkKubernetesNodesReady(); err != nil {
		return err
	}

	if err := c.checkLonghornNodesReady(); err != nil {
		return err
	}

	return nil
}

func (c *Checker) Output() error {
	output, err := formatLogCollection(c.collection)
	if err != nil {
		return errors.Wrap(err, "failed to format upgrade check result")
	}

	if c.OutputFilePath == "" {
		fmt.Print(output)
		return nil
	}

	_, err = commonio.CreateDirectory(filepath.Dir(c.OutputFilePath), time.Now())
	if err != nil {
		return errors.Wrap(err, "failed to create directory")
	}

	c.logger.Debug("Writing result to file")
	if err := os.WriteFile(c.OutputFilePath, []byte(output), 0644); err != nil {
		return errors.Wrap(err, "failed to write output file")
	}

	return nil
}

func (c *Checker) runV1Checks() error {
	// V1 checks attached volumes for:
	// - healthy robustness
	// - non-standby state
	// - no expansion in progress
	// - no migration in progress
	// - non-strict-local data locality

	volumes, err := c.longhornClient.ListVolumesByDataEngine(lhtypes.DataEngineTypeV1)
	if err != nil {
		return errors.Wrap(err, "failed to list v1 volumes")
	}

	attachedVolumeCount := 0
	for _, volume := range volumes.Items {
		if volume.Status.State != lhtypes.VolumeStateAttached {
			continue
		}

		attachedVolumeCount++
		if volume.Status.Robustness != lhtypes.VolumeRobustnessHealthy {
			c.addVolumeCheckReject(volume.Name,
				"volume is not healthy")
		}
		if volume.Status.IsStandby {
			c.addVolumeCheckReject(volume.Name,
				"standby volume does not support live upgrade")
		}
		if volume.Status.ExpansionRequired {
			c.addVolumeCheckReject(volume.Name,
				"volume expansion is in progress")
		}
		if lhutil.IsVolumeMigrating(&volume) {
			c.addVolumeCheckReject(volume.Name,
				"volume migration is in progress")
		}
		if volume.Spec.DataLocality == lhtypes.DataLocalityStrictLocal {
			c.addVolumeCheckReject(volume.Name,
				"strict-local volume does not support live upgrade")
		}
	}

	if attachedVolumeCount == 0 {
		c.addCheckPass(
			"no attached v1 volumes require live-upgrade checks")
	} else {
		c.addCheckPass(
			"checked attached v1 volumes for live-upgrade conditions")
	}

	return nil
}

func (c *Checker) runV2Checks() error {
	// V2 checks attached volumes for:
	// - more than one replica configured
	// - current engine node presence
	// - current engine existence and running state
	// - at least one replica on another node
	// - at least one healthy running replica on another node
	// - running v2 instance manager on the source node
	// - running v2 instance manager on a candidate temporary node

	volumes, err := c.longhornClient.ListVolumesByDataEngine(lhtypes.DataEngineTypeV2)
	if err != nil {
		return errors.Wrap(err, "failed to list v2 volumes")
	}

	attachedVolumeCount := 0
	for _, volume := range volumes.Items {
		if volume.Status.State != lhtypes.VolumeStateAttached {
			continue
		}

		attachedVolumeCount++
		if volume.Spec.NumberOfReplicas < 2 {
			c.addVolumeCheckReject(volume.Name,
				"single-replica volume does not support live upgrade")
			continue
		}
		if volume.Status.CurrentEngineNodeID == "" {
			c.addVolumeCheckReject(volume.Name,
				"current engine node is missing")
			continue
		}

		engine, err := c.getCurrentEngine(&volume)
		if err != nil {
			c.addVolumeCheckReject(volume.Name, err.Error())
			continue
		}
		if engine.Status.CurrentState != lhtypes.InstanceStateRunning {
			c.addVolumeCheckReject(volume.Name,
				"current engine is not running")
		}

		replicas, err := c.longhornClient.ListVolumeReplicas(volume.Name)
		if err != nil {
			return errors.Wrapf(err, "failed to list replicas for volume %v", volume.Name)
		}

		replicaNodes := map[string]struct{}{}
		healthyReplicaNodes := map[string]struct{}{}
		for _, replica := range replicas.Items {
			if replica.Spec.NodeID == "" || replica.Spec.NodeID == volume.Status.CurrentEngineNodeID {
				continue
			}
			replicaNodes[replica.Spec.NodeID] = struct{}{}
			if replica.Status.CurrentState != lhtypes.InstanceStateRunning {
				continue
			}
			if replica.Spec.FailedAt != "" || replica.Spec.HealthyAt == "" {
				continue
			}
			healthyReplicaNodes[replica.Spec.NodeID] = struct{}{}
		}

		if len(replicaNodes) == 0 {
			c.addVolumeCheckReject(volume.Name,
				"no replica is available on another node")
			continue
		}
		if len(healthyReplicaNodes) == 0 {
			c.addVolumeCheckReject(volume.Name,
				"no healthy running replica is available on another node")
			continue
		}

		sourceIMReady, candidateIMReady, err := c.checkV2InstanceManagerReadiness(volume.Status.CurrentEngineNodeID, healthyReplicaNodes)
		if err != nil {
			return errors.Wrapf(err, "failed to check instance manager readiness for volume %v", volume.Name)
		}
		if !sourceIMReady {
			c.addVolumeCheckReject(volume.Name,
				"source node does not have a running v2 instance manager")
		}
		if !candidateIMReady {
			c.addVolumeCheckReject(volume.Name,
				"no healthy replica node has a running v2 instance manager")
		}
	}

	if attachedVolumeCount == 0 {
		c.addCheckPass(
			"no attached v2 volumes require live-upgrade checks")
	} else {
		c.addCheckPass(
			"checked attached v2 volumes for live-upgrade conditions")
	}

	return nil
}

func (c *Checker) addCheckPass(message string) {
	c.collection.Info = append(c.collection.Info, message)
}

func (c *Checker) addClusterCheckReject(message string) {
	c.collection.Error = append(c.collection.Error, message)
}

func (c *Checker) addVolumeCheckReject(volumeName, message string) {
	c.collection.Error = append(c.collection.Error, fmt.Sprintf("volume %s: %s", volumeName, message))
}

func formatLogCollection(collection *types.LogCollection) (string, error) {
	if collection == nil {
		return "", nil
	}

	var output strings.Builder
	writeSection := func(title string, lines []string) {
		if len(lines) == 0 {
			return
		}

		if output.Len() != 0 {
			output.WriteByte('\n')
		}
		output.WriteString(title)
		output.WriteString(":\n")
		for _, line := range lines {
			output.WriteString("- ")
			output.WriteString(line)
			output.WriteByte('\n')
		}
	}

	writeSection("Result", collection.Info)
	writeSection("Warn", collection.Warn)
	writeSection("Error", collection.Error)

	if output.Len() != 0 {
		return output.String(), nil
	}

	return "", nil
}

func (c *Checker) getCurrentEngine(volume *lhtypes.Volume) (*lhtypes.Engine, error) {
	engines, err := c.longhornClient.ListVolumeEngines(volume.Name)
	if err != nil {
		return nil, err
	}

	for _, engine := range engines.Items {
		if engine.Spec.NodeID == volume.Status.CurrentEngineNodeID {
			engineCopy := engine
			return &engineCopy, nil
		}
	}

	return nil, errors.Errorf("current engine was not found on node %v", volume.Status.CurrentEngineNodeID)
}

func (c *Checker) checkKubernetesNodesReady() error {
	c.logger.Debug("Listing Kubernetes nodes")
	nodes, err := c.kubeClient.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return errors.Wrap(err, "failed to list Kubernetes nodes")
	}

	var notReadyNodes []string
	for _, node := range nodes.Items {
		if isKubernetesNodeReady(&node) {
			continue
		}
		notReadyNodes = append(notReadyNodes, node.Name)
	}

	if len(notReadyNodes) != 0 {
		slices.Sort(notReadyNodes)
		c.addClusterCheckReject(fmt.Sprintf("Kubernetes nodes are not Ready: %s", strings.Join(notReadyNodes, ", ")))
		return nil
	}

	c.addCheckPass("all Kubernetes nodes are Ready")
	return nil
}

func (c *Checker) checkLonghornNodesReady() error {
	c.logger.Debug("Listing Longhorn nodes")
	nodes, err := c.longhornClient.ListNodes()
	if err != nil {
		return errors.Wrap(err, "failed to list Longhorn nodes")
	}

	var notReadyNodes []string
	for _, node := range nodes.Items {
		condition := getLonghornNodeCondition(node.Status.Conditions, lhtypes.NodeConditionTypeReady)
		if condition != nil && condition.Status == lhtypes.ConditionStatusTrue {
			continue
		}
		notReadyNodes = append(notReadyNodes, formatLonghornNodeNotReady(node.Name, condition))
	}

	if len(notReadyNodes) != 0 {
		slices.Sort(notReadyNodes)
		c.addClusterCheckReject(fmt.Sprintf("Longhorn nodes are not Ready: %s", strings.Join(notReadyNodes, "; ")))
		return nil
	}

	c.addCheckPass("all Longhorn nodes are Ready")
	return nil
}

func isKubernetesNodeReady(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}

	return false
}

func getLonghornNodeCondition(conditions []lhtypes.Condition, conditionType string) *lhtypes.Condition {
	for _, condition := range conditions {
		if condition.Type == conditionType {
			conditionCopy := condition
			return &conditionCopy
		}
	}

	return nil
}

func formatLonghornNodeNotReady(nodeName string, condition *lhtypes.Condition) string {
	if condition == nil {
		return fmt.Sprintf("%s (missing Ready condition)", nodeName)
	}

	details := []string{}
	if condition.Reason != "" {
		details = append(details, fmt.Sprintf("reason: %s", condition.Reason))
	}
	if condition.Message != "" {
		details = append(details, fmt.Sprintf("message: %s", condition.Message))
	}

	if len(details) == 0 {
		return fmt.Sprintf("%s (status: %s)", nodeName, condition.Status)
	}

	return fmt.Sprintf("%s (%s)", nodeName, strings.Join(details, ", "))
}

func (c *Checker) checkV2InstanceManagerReadiness(sourceNodeID string, healthyReplicaNodes map[string]struct{}) (bool, bool, error) {
	ims, err := c.longhornClient.ListInstanceManagers()
	if err != nil {
		return false, false, err
	}

	sourceIMReady := false
	candidateIMReady := false
	for _, im := range ims.Items {
		if im.Spec.DataEngine != lhtypes.DataEngineTypeV2 ||
			im.Spec.Type != lhtypes.InstanceManagerTypeAllInOne ||
			im.Status.CurrentState != lhtypes.InstanceManagerStateRunning ||
			im.DeletionTimestamp != nil {
			continue
		}

		if im.Spec.NodeID == sourceNodeID {
			sourceIMReady = true
		}

		if _, ok := healthyReplicaNodes[im.Spec.NodeID]; ok {
			candidateIMReady = true
		}
	}

	return sourceIMReady, candidateIMReady, nil
}
