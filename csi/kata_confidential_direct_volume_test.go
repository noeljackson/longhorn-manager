package csi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/client-go/kubernetes/fake"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	testKataConfidentialManifestURI = "kbs:///tenant/storage-manifests/workspace-v1"
	testKataConfidentialPodUID      = "7fd5ae3d-01aa-4c7a-9d4e-a683f648851d"
	testKataConfidentialNodeID      = "test-node"
)

type fakeKataDirectVolumeRuntime struct {
	adds       []kataConfidentialDirectVolumeMountInfo
	addTargets []string
	removes    []string
	stats      []byte
	err        error
}

func (r *fakeKataDirectVolumeRuntime) Add(_ context.Context, targetPath string, mountInfo kataConfidentialDirectVolumeMountInfo) error {
	r.addTargets = append(r.addTargets, targetPath)
	r.adds = append(r.adds, mountInfo)
	return r.err
}

func (r *fakeKataDirectVolumeRuntime) Remove(_ context.Context, targetPath string) error {
	r.removes = append(r.removes, targetPath)
	return r.err
}

func (r *fakeKataDirectVolumeRuntime) Stats(_ context.Context, _ string) ([]byte, error) {
	return r.stats, r.err
}

func testKataConfidentialDirectVolume() *longhornclient.Volume {
	return &longhornclient.Volume{
		AccessMode:       string(longhorn.AccessModeReadWriteOncePod),
		Controllers:      []longhornclient.Controller{{Endpoint: "/dev/longhorn/test-volume"}},
		DataEngine:       string(longhorn.DataEngineTypeV1),
		Frontend:         string(longhorn.VolumeFrontendBlockDev),
		Name:             "test-volume",
		NumberOfReplicas: 1,
		Ready:            true,
		State:            string(longhorn.VolumeStateAttached),
	}
}

func testKataConfidentialDirectVolumeCapability() *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{FsType: defaultFsType},
		},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
		},
	}
}

func testKataConfidentialPVC() *corev1.PersistentVolumeClaim {
	mode := corev1.PersistentVolumeFilesystem
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "workspace",
			Namespace: "sandbox",
			Annotations: map[string]string{
				kataConfidentialStorageManifestURIAnnotation: testKataConfidentialManifestURI,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod},
			VolumeMode:  &mode,
		},
	}
}

func testKataConfidentialPod() *corev1.Pod {
	fsGroup := int64(1000)
	fsGroupChangePolicy := corev1.FSGroupChangeOnRootMismatch
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox",
			Namespace: "sandbox",
			UID:       types.UID(testKataConfidentialPodUID),
		},
		Spec: corev1.PodSpec{
			NodeName: testKataConfidentialNodeID,
			SecurityContext: &corev1.PodSecurityContext{
				FSGroup:             &fsGroup,
				FSGroupChangePolicy: &fsGroupChangePolicy,
			},
			Volumes: []corev1.Volume{{
				Name: "workspace",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "workspace"},
				},
			}},
		},
	}
}

func testKataConfidentialVolumeContext() map[string]string {
	return map[string]string{
		kataConfidentialDirectVolumeParameter: "true",
		csiPVCNameKey:                         "workspace",
		csiPVCNamespaceKey:                    "sandbox",
		csiPodNameKey:                         "sandbox",
		csiPodNamespaceKey:                    "sandbox",
		csiPodUIDKey:                          testKataConfidentialPodUID,
	}
}

func newTestKataConfidentialManager(t *testing.T, runtime kataDirectVolumeRuntime) *kataConfidentialDirectVolumeManager {
	t.Helper()
	return &kataConfidentialDirectVolumeManager{
		stateDir: t.TempDir(),
		runtime:  runtime,
	}
}

func TestKataConfidentialDirectVolumeLifecycle(t *testing.T) {
	ctx := context.Background()
	runtime := &fakeKataDirectVolumeRuntime{
		stats: []byte(`{"usage":[{"available":8,"total":10,"used":2,"unit":1},{"available":80,"total":100,"used":20,"unit":2}],"volume_condition":{"abnormal":false,"message":""}}`),
	}
	manager := newTestKataConfidentialManager(t, runtime)
	kubeletPodsRoot := t.TempDir()
	ns := &NodeServer{
		directVolumes:   manager,
		kubeClient:      fake.NewSimpleClientset(testKataConfidentialPVC(), testKataConfidentialPod()),
		kubeletPodsRoot: kubeletPodsRoot,
		nodeID:          testKataConfidentialNodeID,
		log:             logrus.New().WithField("test", "kata-confidential-direct-volume"),
	}
	volume := testKataConfidentialDirectVolume()
	capability := testKataConfidentialDirectVolumeCapability()
	volumeContext := testKataConfidentialVolumeContext()
	stageRequest := &csi.NodeStageVolumeRequest{
		VolumeId:          volume.Name,
		StagingTargetPath: "/var/lib/kubelet/plugins/kubernetes.io/csi/stage/test-volume",
		VolumeCapability:  capability,
		VolumeContext:     volumeContext,
	}

	for i := 0; i < 2; i++ {
		if _, err := ns.nodeStageKataConfidentialDirectVolume(ctx, stageRequest, volume); err != nil {
			t.Fatalf("stage attempt %d failed: %v", i+1, err)
		}
	}
	managed, err := manager.IsManaged(volume.Name)
	if err != nil || !managed {
		t.Fatalf("expected staged volume to be managed, managed=%v err=%v", managed, err)
	}

	targetPath := filepath.Join(kubeletPodsRoot, testKataConfidentialPodUID, "volumes", "kubernetes.io~csi", "pvc-id", "mount")
	publishRequest := &csi.NodePublishVolumeRequest{
		VolumeId:          volume.Name,
		StagingTargetPath: stageRequest.StagingTargetPath,
		TargetPath:        targetPath,
		VolumeCapability:  capability,
		VolumeContext:     volumeContext,
	}
	for i := 0; i < 2; i++ {
		if _, err := ns.nodePublishKataConfidentialDirectVolume(ctx, publishRequest, volume); err != nil {
			t.Fatalf("publish attempt %d failed: %v", i+1, err)
		}
	}
	if len(runtime.adds) != 2 || runtime.addTargets[0] != targetPath || runtime.addTargets[1] != targetPath {
		t.Fatalf("expected two idempotent Kata registrations, got targets %#v", runtime.addTargets)
	}
	request := runtime.adds[0].ConfidentialStorage
	if request == nil || request.ManifestURI != testKataConfidentialManifestURI || request.RequestedAccess != "readWrite" {
		t.Fatalf("unexpected confidential storage contract: %#v", request)
	}
	if runtime.adds[0].VolumeType != "directvol" || runtime.adds[0].FsType != kataConfidentialStorageFSType || runtime.adds[0].Device != volume.Controllers[0].Endpoint {
		t.Fatalf("unexpected mount info: %#v", runtime.adds[0])
	}
	if !reflect.DeepEqual(runtime.adds[0].Metadata, map[string]string{"fsGroup": "1000", "fsGroupChangePolicy": "OnRootMismatch"}) {
		t.Fatalf("unexpected guest filesystem group metadata: %#v", runtime.adds[0].Metadata)
	}
	if info, err := os.Lstat(targetPath); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("expected empty host target directory, info=%#v err=%v", info, err)
	}
	if entries, err := os.ReadDir(targetPath); err != nil || len(entries) != 0 {
		t.Fatalf("expected empty host target directory, entries=%#v err=%v", entries, err)
	}

	stateData, err := os.ReadFile(manager.statePath(volume.Name))
	if err != nil {
		t.Fatal(err)
	}
	stateInfo, err := os.Stat(manager.statePath(volume.Name))
	if err != nil {
		t.Fatal(err)
	}
	if stateInfo.Mode().Perm() != 0600 {
		t.Fatalf("unexpected lifecycle state permissions: %o", stateInfo.Mode().Perm())
	}
	stateDirInfo, err := os.Stat(manager.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	if stateDirInfo.Mode().Perm() != 0700 {
		t.Fatalf("unexpected lifecycle state directory permissions: %o", stateDirInfo.Mode().Perm())
	}
	if strings.Contains(string(stateData), testKataConfidentialManifestURI) || strings.Contains(string(stateData), "kbs://") {
		t.Fatalf("lifecycle state contains manifest metadata: %s", stateData)
	}
	var state kataConfidentialDirectVolumeState
	if err := json.Unmarshal(stateData, &state); err != nil {
		t.Fatal(err)
	}
	if len(state.PublishedPaths) != 1 || state.PublishedPaths[0] != targetPath {
		t.Fatalf("unexpected published paths: %#v", state.PublishedPaths)
	}

	stats, err := ns.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{VolumeId: volume.Name, VolumePath: targetPath})
	if err != nil {
		t.Fatal(err)
	}
	if len(stats.Usage) != 2 || stats.Usage[0].Total != 10 || stats.Usage[0].Unit != csi.VolumeUsage_BYTES || stats.Usage[1].Total != 100 || stats.Usage[1].Unit != csi.VolumeUsage_INODES {
		t.Fatalf("unexpected direct-volume stats: %#v", stats)
	}

	_, err = ns.NodeExpandVolume(ctx, &csi.NodeExpandVolumeRequest{
		VolumeId:         volume.Name,
		CapacityRange:    &csi.CapacityRange{RequiredBytes: 20},
		VolumeCapability: capability,
	})
	if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != kataConfidentialDirectVolumeResizeUnsupported {
		t.Fatalf("unexpected direct-volume expansion error: %v", err)
	}
	stateAfterResize, err := os.ReadFile(manager.statePath(volume.Name))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stateData, stateAfterResize) {
		t.Fatal("resize rejection mutated confidential direct-volume lifecycle state")
	}

	for i := 0; i < 2; i++ {
		if _, err := ns.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: volume.Name, TargetPath: targetPath}); err != nil {
			t.Fatalf("unpublish attempt %d failed: %v", i+1, err)
		}
	}
	if len(runtime.removes) != 1 {
		t.Fatalf("expected one Kata removal and an idempotent no-op, got %#v", runtime.removes)
	}
	if _, err := os.Lstat(targetPath); !os.IsNotExist(err) {
		t.Fatalf("expected target cleanup, got %v", err)
	}
	if _, err := ns.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: volume.Name, StagingTargetPath: stageRequest.StagingTargetPath}); err != nil {
		t.Fatal(err)
	}
	managed, err = manager.IsManaged(volume.Name)
	if err != nil || managed {
		t.Fatalf("expected lifecycle state removal, managed=%v err=%v", managed, err)
	}
}

func TestKataConfidentialDirectVolumeUnstageCleansLingeringRegistration(t *testing.T) {
	ctx := context.Background()
	runtime := &fakeKataDirectVolumeRuntime{}
	manager := newTestKataConfidentialManager(t, runtime)
	if err := manager.Stage("volume", "/stage", "/dev/longhorn/volume"); err != nil {
		t.Fatal(err)
	}
	info := kataConfidentialDirectVolumeMountInfo{VolumeType: "directvol", Device: "/dev/longhorn/volume", FsType: kataConfidentialStorageFSType}
	targetPath := filepath.Join(t.TempDir(), "target")
	if err := manager.Publish(ctx, "volume", targetPath, info); err != nil {
		t.Fatal(err)
	}
	if err := manager.Unstage(ctx, "volume"); err != nil {
		t.Fatal(err)
	}
	if len(runtime.removes) != 1 || runtime.removes[0] != targetPath {
		t.Fatalf("unexpected cleanup calls: %#v", runtime.removes)
	}
	if _, err := os.Lstat(targetPath); !os.IsNotExist(err) {
		t.Fatalf("expected lingering target cleanup, got %v", err)
	}
}

func TestValidateKataConfidentialDirectVolumeRejectsUnsupportedModes(t *testing.T) {
	baseVolume := testKataConfidentialDirectVolume()

	tests := []struct {
		name   string
		mutate func(*longhornclient.Volume, *csi.VolumeCapability)
	}{
		{name: "block volume mode", mutate: func(_ *longhornclient.Volume, c *csi.VolumeCapability) {
			c.AccessType = &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}
		}},
		{name: "rwo access", mutate: func(v *longhornclient.Volume, _ *csi.VolumeCapability) {
			v.AccessMode = string(longhorn.AccessModeReadWriteOnce)
		}},
		{name: "non-rwop CSI mode", mutate: func(_ *longhornclient.Volume, c *csi.VolumeCapability) {
			c.AccessMode.Mode = csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
		}},
		{name: "host encryption", mutate: func(v *longhornclient.Volume, _ *csi.VolumeCapability) { v.Encrypted = true }},
		{name: "migration", mutate: func(v *longhornclient.Volume, _ *csi.VolumeCapability) { v.Migratable = true }},
		{name: "multiple replicas", mutate: func(v *longhornclient.Volume, _ *csi.VolumeCapability) { v.NumberOfReplicas = 2 }},
		{name: "v2", mutate: func(v *longhornclient.Volume, _ *csi.VolumeCapability) {
			v.DataEngine = string(longhorn.DataEngineTypeV2)
		}},
		{name: "ublk", mutate: func(v *longhornclient.Volume, _ *csi.VolumeCapability) {
			v.Frontend = string(longhorn.VolumeFrontendUblk)
		}},
		{name: "mount flags", mutate: func(_ *longhornclient.Volume, c *csi.VolumeCapability) { c.GetMount().MountFlags = []string{"discard"} }},
		{name: "xfs", mutate: func(_ *longhornclient.Volume, c *csi.VolumeCapability) { c.GetMount().FsType = "xfs" }},
		{name: "invalid volume ID", mutate: func(v *longhornclient.Volume, _ *csi.VolumeCapability) {
			v.Name = "tenant//volume"
		}},
		{name: "relative endpoint", mutate: func(v *longhornclient.Volume, _ *csi.VolumeCapability) {
			v.Controllers[0].Endpoint = "dev/longhorn/volume"
		}},
		{name: "non-Longhorn endpoint", mutate: func(v *longhornclient.Volume, _ *csi.VolumeCapability) {
			v.Controllers[0].Endpoint = "/dev/nvme0n1"
		}},
		{name: "detached", mutate: func(v *longhornclient.Volume, _ *csi.VolumeCapability) {
			v.State = string(longhorn.VolumeStateDetached)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			volume := *baseVolume
			volume.Controllers = append([]longhornclient.Controller(nil), baseVolume.Controllers...)
			capability := testKataConfidentialDirectVolumeCapability()
			test.mutate(&volume, capability)
			if _, err := validateKataConfidentialDirectVolume(&volume, capability); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestKataConfidentialStorageManifestURIValidation(t *testing.T) {
	ns := &NodeServer{kubeClient: fake.NewSimpleClientset(testKataConfidentialPVC())}
	manifestURI, err := ns.kataConfidentialStorageManifestURI(context.Background(), testKataConfidentialVolumeContext())
	if err != nil {
		t.Fatal(err)
	}
	if manifestURI != testKataConfidentialManifestURI {
		t.Fatalf("unexpected confidential storage manifest URI: %q", manifestURI)
	}

	for _, invalidManifestURI := range []string{"", "https://example.invalid/manifest", "kbs:///tenant/manifests/latest?revision=1", "kbs:///tenant/manifests/workspace/extra"} {
		invalidClaim := testKataConfidentialPVC()
		invalidClaim.Annotations[kataConfidentialStorageManifestURIAnnotation] = invalidManifestURI
		ns.kubeClient = fake.NewSimpleClientset(invalidClaim)
		if _, err := ns.kataConfidentialStorageManifestURI(context.Background(), testKataConfidentialVolumeContext()); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected manifest URI %q rejection, got %v", invalidManifestURI, err)
		}
	}
	for _, legacyAnnotation := range []string{kataConfidentialStorageLegacyKeyURIAnnotation, kataConfidentialStorageLegacyVolumeIDAnnotation} {
		invalidClaim := testKataConfidentialPVC()
		invalidClaim.Annotations[legacyAnnotation] = "legacy-value"
		ns.kubeClient = fake.NewSimpleClientset(invalidClaim)
		if _, err := ns.kataConfidentialStorageManifestURI(context.Background(), testKataConfidentialVolumeContext()); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected legacy annotation %q rejection, got %v", legacyAnnotation, err)
		}
	}
}

func TestKataConfidentialPodMountMetadataValidation(t *testing.T) {
	kubeletPodsRoot := t.TempDir()
	targetPath := filepath.Join(kubeletPodsRoot, testKataConfidentialPodUID, "volumes", "kubernetes.io~csi", "pvc-id", "mount")
	baseContext := testKataConfidentialVolumeContext()

	for _, test := range []struct {
		name          string
		mutateContext func(map[string]string)
		mutatePod     func(*corev1.Pod)
		targetPath    string
	}{
		{name: "valid"},
		{name: "missing Pod metadata", mutateContext: func(context map[string]string) { delete(context, csiPodUIDKey) }},
		{name: "Pod and PVC namespace differ", mutateContext: func(context map[string]string) { context[csiPodNamespaceKey] = "other" }},
		{name: "Pod UID changed", mutateContext: func(context map[string]string) { context[csiPodUIDKey] = "different" }},
		{name: "wrong node", mutatePod: func(pod *corev1.Pod) { pod.Spec.NodeName = "other-node" }},
		{name: "PVC not referenced", mutatePod: func(pod *corev1.Pod) { pod.Spec.Volumes = nil }},
		{name: "target outside Pod directory", targetPath: filepath.Join(kubeletPodsRoot, "other", "volumes", "kubernetes.io~csi", "pvc-id", "mount")},
		{name: "negative fsGroup", mutatePod: func(pod *corev1.Pod) { group := int64(-1); pod.Spec.SecurityContext.FSGroup = &group }},
		{name: "fsGroup above guest range", mutatePod: func(pod *corev1.Pod) { group := int64(1) << 32; pod.Spec.SecurityContext.FSGroup = &group }},
		{name: "invalid fsGroup change policy", mutatePod: func(pod *corev1.Pod) {
			policy := corev1.PodFSGroupChangePolicy("Sometimes")
			pod.Spec.SecurityContext.FSGroupChangePolicy = &policy
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			volumeContext := make(map[string]string, len(baseContext))
			for key, value := range baseContext {
				volumeContext[key] = value
			}
			if test.mutateContext != nil {
				test.mutateContext(volumeContext)
			}
			pod := testKataConfidentialPod()
			if test.mutatePod != nil {
				test.mutatePod(pod)
			}
			requestedTarget := targetPath
			if test.targetPath != "" {
				requestedTarget = test.targetPath
			}
			ns := &NodeServer{
				kubeClient:      fake.NewSimpleClientset(pod),
				kubeletPodsRoot: kubeletPodsRoot,
				nodeID:          testKataConfidentialNodeID,
			}
			metadata, err := ns.kataConfidentialPodMountMetadata(context.Background(), volumeContext, requestedTarget)
			if test.name == "valid" {
				if err != nil || !reflect.DeepEqual(metadata, map[string]string{"fsGroup": "1000", "fsGroupChangePolicy": "OnRootMismatch"}) {
					t.Fatalf("unexpected valid metadata: %#v err=%v", metadata, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected validation failure, got %#v", metadata)
			}
		})
	}

	pod := testKataConfidentialPod()
	pod.Spec.SecurityContext = nil
	ns := &NodeServer{kubeClient: fake.NewSimpleClientset(pod), kubeletPodsRoot: kubeletPodsRoot, nodeID: testKataConfidentialNodeID}
	metadata, err := ns.kataConfidentialPodMountMetadata(context.Background(), baseContext, targetPath)
	if err != nil || len(metadata) != 0 {
		t.Fatalf("Pod without fsGroup should produce no guest group metadata: %#v err=%v", metadata, err)
	}
}

func TestKataConfidentialDirectVolumeTargetFailsClosed(t *testing.T) {
	root := t.TempDir()
	fileTarget := filepath.Join(root, "file")
	if err := os.WriteFile(fileTarget, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := ensureKataConfidentialDirectVolumeTarget(fileTarget); err == nil {
		t.Fatal("expected regular target file rejection")
	}
	symlinkTarget := filepath.Join(root, "symlink")
	if err := os.Symlink(root, symlinkTarget); err != nil {
		t.Fatal(err)
	}
	if err := ensureKataConfidentialDirectVolumeTarget(symlinkTarget); err == nil {
		t.Fatal("expected target symlink rejection")
	}
	nonEmptyTarget := filepath.Join(root, "non-empty")
	if err := os.Mkdir(nonEmptyTarget, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nonEmptyTarget, "data"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := removeKataConfidentialDirectVolumeTarget(nonEmptyTarget); err == nil {
		t.Fatal("expected non-empty target cleanup rejection")
	}
}

func TestKataConfidentialDirectVolumeMarkerFailsClosed(t *testing.T) {
	requested, err := kataConfidentialDirectVolumeRequested(nil)
	if err != nil || requested {
		t.Fatalf("unmarked volume changed behavior: requested=%v err=%v", requested, err)
	}
	for _, value := range []string{"", "True", "false", "1"} {
		if _, err := kataConfidentialDirectVolumeRequested(map[string]string{kataConfidentialDirectVolumeParameter: value}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("marker %q did not fail closed: %v", value, err)
		}
	}
	manager := newTestKataConfidentialManager(t, &fakeKataDirectVolumeRuntime{})
	if err := manager.Stage("managed-volume", "/stage", "/dev/longhorn/managed-volume"); err != nil {
		t.Fatal(err)
	}
	ns := &NodeServer{directVolumes: manager}
	if _, err := ns.kataConfidentialDirectVolumeMode("managed-volume", nil); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("managed volume without marker did not fail closed: %v", err)
	}
	requested, err = ns.kataConfidentialDirectVolumeMode("standard-volume", nil)
	if err != nil || requested {
		t.Fatalf("standard volume changed behavior: requested=%v err=%v", requested, err)
	}
}

func TestKataConfidentialDirectVolumeStatsValidation(t *testing.T) {
	for _, test := range []struct {
		name  string
		stats string
	}{
		{name: "unknown unit", stats: `{"usage":[{"total":1,"unit":0}]}`},
		{name: "signed overflow", stats: `{"usage":[{"total":` + "9223372036854775808" + `,"unit":1}]}`},
		{name: "invalid json", stats: `{`},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := newTestKataConfidentialManager(t, &fakeKataDirectVolumeRuntime{stats: []byte(test.stats)})
			if err := manager.Stage("volume", "/stage", "/dev/longhorn/volume"); err != nil {
				t.Fatal(err)
			}
			info := kataConfidentialDirectVolumeMountInfo{VolumeType: "directvol", Device: "/dev/longhorn/volume", FsType: kataConfidentialStorageFSType}
			targetPath := filepath.Join(t.TempDir(), "target")
			if err := manager.Publish(context.Background(), "volume", targetPath, info); err != nil {
				t.Fatal(err)
			}
			if _, err := manager.Stats(context.Background(), "volume", targetPath); err == nil {
				t.Fatal("expected invalid stats error")
			}
		})
	}
	manager := newTestKataConfidentialManager(t, &fakeKataDirectVolumeRuntime{stats: []byte(`{"usage":[{"total":1,"unit":1}]}`), err: errors.New("runtime unavailable")})
	if err := manager.Stage("volume", "/stage", "/dev/longhorn/volume"); err != nil {
		t.Fatal(err)
	}
	info := kataConfidentialDirectVolumeMountInfo{VolumeType: "directvol", Device: "/dev/longhorn/volume", FsType: kataConfidentialStorageFSType}
	targetPath := filepath.Join(t.TempDir(), "target")
	if err := manager.Publish(context.Background(), "volume", targetPath, info); err == nil {
		t.Fatal("expected runtime add error")
	}
	if _, err := manager.Stats(context.Background(), "volume", targetPath); err == nil {
		t.Fatal("expected runtime error")
	}
}

func TestHostKataCtlUsesHostRootAndPreservesBoundedDiagnostics(t *testing.T) {
	var command string
	var args []string
	runtime := &hostKataCtl{run: func(_ context.Context, gotCommand string, gotArgs ...string) ([]byte, error) {
		command = gotCommand
		args = append([]string(nil), gotArgs...)
		return []byte("structural failure marker"), errors.New("exit status 1")
	}}
	err := runtime.Add(context.Background(), "/target", kataConfidentialDirectVolumeMountInfo{
		VolumeType: "directvol",
		Device:     "/dev/longhorn/volume",
		FsType:     kataConfidentialStorageFSType,
		Metadata:   map[string]string{"fsGroup": "1000", "fsGroupChangePolicy": "OnRootMismatch"},
		ConfidentialStorage: &kataConfidentialStorageContract{
			ManifestURI:     testKataConfidentialManifestURI,
			RequestedAccess: "readWrite",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "structural failure marker") || !strings.Contains(err.Error(), "exit status 1") {
		t.Fatalf("runtime error lost its diagnostic: %v", err)
	}
	if command != nsMounterPath || len(args) != 6 || args[0] != "--host-root" || args[1] != kataCtlPath || args[2] != "direct-volume" || args[3] != "add" || args[4] != "/target" {
		t.Fatalf("unexpected host Kata command: %q %#v", command, args)
	}
	wantMountInfo := `{"volume-type":"directvol","device":"/dev/longhorn/volume","fstype":"confidential-storage","metadata":{"fsGroup":"1000","fsGroupChangePolicy":"OnRootMismatch"},"confidential-storage":{"manifest-uri":"kbs:///tenant/storage-manifests/workspace-v1","requested-access":"readWrite"}}`
	if args[5] != wantMountInfo {
		t.Fatalf("unexpected typed Kata mount contract: %s", args[5])
	}
}

func TestHostKataCtlUsesConfiguredPath(t *testing.T) {
	var args []string
	runtime := &hostKataCtl{
		path: "/usr/local/bin/kata-ctl",
		run: func(_ context.Context, _ string, gotArgs ...string) ([]byte, error) {
			args = append([]string(nil), gotArgs...)
			return nil, nil
		},
	}
	if err := runtime.Remove(context.Background(), "/target"); err != nil {
		t.Fatal(err)
	}
	want := []string{"--host-root", "/usr/local/bin/kata-ctl", "direct-volume", "remove", "/target"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("configured Kata control path was not used: %#v", args)
	}
	args = nil
	if _, err := runtime.Stats(context.Background(), "/target"); err != nil {
		t.Fatal(err)
	}
	want = []string{"--host-root", "/usr/local/bin/kata-ctl", "direct-volume", "stats", "/target"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("Kata stats did not use the positional runtime-rs contract: %#v", args)
	}
}

func TestValidateKataCtlPath(t *testing.T) {
	for _, path := range []string{DefaultKataCtlPath, "/usr/local/bin/kata-ctl"} {
		if err := ValidateKataCtlPath(path); err != nil {
			t.Fatalf("expected %q to be valid: %v", path, err)
		}
	}
	for _, path := range []string{"", "kata-ctl", "/", "/usr/local/../bin/kata-ctl"} {
		if err := ValidateKataCtlPath(path); err == nil {
			t.Fatalf("expected %q to be rejected", path)
		}
	}
}

func TestBoundedKataCommandOutput(t *testing.T) {
	message := boundedKataCommandOutput(append(bytes.Repeat([]byte("a"), 9000), 0))
	if !strings.HasSuffix(message, " [truncated]") || len(message) > 8300 {
		t.Fatalf("unexpected bounded diagnostic length or marker: %d %q", len(message), message[len(message)-20:])
	}
	if boundedKataCommandOutput([]byte("failure\x00marker")) != "failure�marker" {
		t.Fatal("command diagnostic did not neutralize a control character")
	}
}

func TestNSMounterHostRootUsesTalosKubeletNamespacesAndPID1Root(t *testing.T) {
	procDir := filepath.Join(t.TempDir(), "proc")
	if err := os.MkdirAll(filepath.Join(procDir, "123", "ns"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(procDir, "version"), []byte("Linux talos"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(procDir, "123", "status"), []byte("Name:\tkubelet\n"), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(filepath.Join("..", "package", "nsmounter"), "--host-root", kataCtlPath, "direct-volume", "remove", "/target")
	command.Env = append(os.Environ(), "PROC_DIR="+procDir, "NSENTER_BIN=/bin/echo")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("nsmounter harness failed: %v: %s", err, output)
	}
	want := strings.Join([]string{
		"--mount=" + filepath.Join(procDir, "123", "ns", "mnt"),
		"--net=" + filepath.Join(procDir, "123", "ns", "net"),
		"--uts=" + filepath.Join(procDir, "123", "ns", "uts"),
		"--root=" + filepath.Join(procDir, "1", "root"),
		"--wd=/",
		"--",
		kataCtlPath,
		"direct-volume",
		"remove",
		"/target",
	}, " ") + "\n"
	if string(output) != want {
		t.Fatalf("unexpected nsmounter invocation:\n%s\nwant:\n%s", output, want)
	}
}
