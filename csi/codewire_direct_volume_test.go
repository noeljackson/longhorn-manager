package csi

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/client-go/kubernetes/fake"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const testCodewireKeyID = "123e4567-e89b-12d3-a456-426614174000"

type fakeKataDirectVolumeRuntime struct {
	adds       []codewireDirectVolumeMountInfo
	addTargets []string
	removes    []string
	stats      []byte
	err        error
}

func (r *fakeKataDirectVolumeRuntime) Add(_ context.Context, targetPath string, mountInfo codewireDirectVolumeMountInfo) error {
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

func testCodewireDirectVolume() *longhornclient.Volume {
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

func testCodewireDirectVolumeCapability() *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{FsType: defaultFsType},
		},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
		},
	}
}

func testCodewirePVC() *corev1.PersistentVolumeClaim {
	mode := corev1.PersistentVolumeFilesystem
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "workspace",
			Namespace: "sandbox",
			Annotations: map[string]string{
				codewireStorageKeyIDAnnotation: testCodewireKeyID,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod},
			VolumeMode:  &mode,
		},
	}
}

func testCodewireVolumeContext() map[string]string {
	return map[string]string{
		codewireKataDirectVolumeParameter: "true",
		csiPVCNameKey:                     "workspace",
		csiPVCNamespaceKey:                "sandbox",
	}
}

func newTestCodewireManager(t *testing.T, runtime codewireKataRuntime) *codewireDirectVolumeManager {
	t.Helper()
	return &codewireDirectVolumeManager{
		stateDir: t.TempDir(),
		runtime:  runtime,
	}
}

func TestCodewireDirectVolumeLifecycle(t *testing.T) {
	ctx := context.Background()
	runtime := &fakeKataDirectVolumeRuntime{
		stats: []byte(`{"usage":[{"available":8,"total":10,"used":2,"unit":1},{"available":80,"total":100,"used":20,"unit":2}],"volume_condition":{"abnormal":false,"message":""}}`),
	}
	manager := newTestCodewireManager(t, runtime)
	ns := &NodeServer{
		directVolumes: manager,
		kubeClient:    fake.NewSimpleClientset(testCodewirePVC()),
		log:           logrus.New().WithField("test", "codewire-direct-volume"),
	}
	volume := testCodewireDirectVolume()
	capability := testCodewireDirectVolumeCapability()
	volumeContext := testCodewireVolumeContext()
	stageRequest := &csi.NodeStageVolumeRequest{
		VolumeId:          volume.Name,
		StagingTargetPath: "/var/lib/kubelet/plugins/kubernetes.io/csi/stage/test-volume",
		VolumeCapability:  capability,
		VolumeContext:     volumeContext,
	}

	for i := 0; i < 2; i++ {
		if _, err := ns.nodeStageCodewireDirectVolume(ctx, stageRequest, volume); err != nil {
			t.Fatalf("stage attempt %d failed: %v", i+1, err)
		}
	}
	managed, err := manager.IsManaged(volume.Name)
	if err != nil || !managed {
		t.Fatalf("expected staged volume to be managed, managed=%v err=%v", managed, err)
	}

	targetPath := "/var/lib/kubelet/pods/pod-id/volumes/kubernetes.io~csi/workspace/mount"
	publishRequest := &csi.NodePublishVolumeRequest{
		VolumeId:          volume.Name,
		StagingTargetPath: stageRequest.StagingTargetPath,
		TargetPath:        targetPath,
		VolumeCapability:  capability,
		VolumeContext:     volumeContext,
	}
	for i := 0; i < 2; i++ {
		if _, err := ns.nodePublishCodewireDirectVolume(ctx, publishRequest, volume); err != nil {
			t.Fatalf("publish attempt %d failed: %v", i+1, err)
		}
	}
	if len(runtime.adds) != 2 || runtime.addTargets[0] != targetPath || runtime.addTargets[1] != targetPath {
		t.Fatalf("expected two idempotent Kata registrations, got targets %#v", runtime.addTargets)
	}
	wantMetadata := map[string]string{
		codewireStorageEncryptionKey: "luks2",
		codewireStorageSourceKey:     "auto",
		codewireStorageKeyURIKey:     codewireStorageKeyURIPrefix + testCodewireKeyID,
		codewireStorageFilesystemKey: "ext4",
		codewireStorageGrowKey:       "true",
	}
	if len(runtime.adds[0].Metadata) != len(wantMetadata) {
		t.Fatalf("unexpected metadata count: got %#v", runtime.adds[0].Metadata)
	}
	for key, value := range wantMetadata {
		if runtime.adds[0].Metadata[key] != value {
			t.Fatalf("unexpected metadata %s=%q", key, runtime.adds[0].Metadata[key])
		}
	}
	if runtime.adds[0].VolumeType != "block" || runtime.adds[0].FsType != "ext4" || runtime.adds[0].Device != volume.Controllers[0].Endpoint || len(runtime.adds[0].Options) != 0 {
		t.Fatalf("unexpected mount info: %#v", runtime.adds[0])
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
	if strings.Contains(string(stateData), testCodewireKeyID) || strings.Contains(string(stateData), "kbs://") {
		t.Fatalf("lifecycle state contains key metadata: %s", stateData)
	}
	var state codewireDirectVolumeState
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
	if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != codewireDirectVolumeRestartRequired {
		t.Fatalf("unexpected direct-volume expansion error: %v", err)
	}

	for i := 0; i < 2; i++ {
		if _, err := ns.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: volume.Name, TargetPath: targetPath}); err != nil {
			t.Fatalf("unpublish attempt %d failed: %v", i+1, err)
		}
	}
	if len(runtime.removes) != 1 {
		t.Fatalf("expected one Kata removal and an idempotent no-op, got %#v", runtime.removes)
	}
	if _, err := ns.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: volume.Name, StagingTargetPath: stageRequest.StagingTargetPath}); err != nil {
		t.Fatal(err)
	}
	managed, err = manager.IsManaged(volume.Name)
	if err != nil || managed {
		t.Fatalf("expected lifecycle state removal, managed=%v err=%v", managed, err)
	}
}

func TestCodewireDirectVolumeUnstageCleansLingeringRegistration(t *testing.T) {
	ctx := context.Background()
	runtime := &fakeKataDirectVolumeRuntime{}
	manager := newTestCodewireManager(t, runtime)
	if err := manager.Stage("volume", "/stage", "/dev/longhorn/volume"); err != nil {
		t.Fatal(err)
	}
	info := codewireDirectVolumeMountInfo{VolumeType: "block", Device: "/dev/longhorn/volume", FsType: "ext4", Metadata: map[string]string{}, Options: []string{}}
	if err := manager.Publish(ctx, "volume", "/target", info); err != nil {
		t.Fatal(err)
	}
	if err := manager.Unstage(ctx, "volume"); err != nil {
		t.Fatal(err)
	}
	if len(runtime.removes) != 1 || runtime.removes[0] != "/target" {
		t.Fatalf("unexpected cleanup calls: %#v", runtime.removes)
	}
}

func TestValidateCodewireDirectVolumeRejectsUnsupportedModes(t *testing.T) {
	baseVolume := testCodewireDirectVolume()

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
			capability := testCodewireDirectVolumeCapability()
			test.mutate(&volume, capability)
			if _, err := validateCodewireDirectVolume(&volume, capability); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestCodewireDirectVolumeMetadataValidation(t *testing.T) {
	ns := &NodeServer{kubeClient: fake.NewSimpleClientset(testCodewirePVC())}
	metadata, err := ns.codewireDirectVolumeMetadata(context.Background(), testCodewireVolumeContext())
	if err != nil {
		t.Fatal(err)
	}
	if len(metadata) != 5 || metadata[codewireStorageKeyURIKey] != codewireStorageKeyURIPrefix+testCodewireKeyID {
		t.Fatalf("unexpected metadata: %#v", metadata)
	}

	invalidClaim := testCodewirePVC()
	invalidClaim.Annotations[codewireStorageKeyIDAnnotation] = strings.ToUpper(testCodewireKeyID)
	ns.kubeClient = fake.NewSimpleClientset(invalidClaim)
	if _, err := ns.codewireDirectVolumeMetadata(context.Background(), testCodewireVolumeContext()); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected canonical UUID rejection, got %v", err)
	}
}

func TestCodewireDirectVolumeMarkerFailsClosed(t *testing.T) {
	requested, err := codewireDirectVolumeRequested(nil)
	if err != nil || requested {
		t.Fatalf("unmarked volume changed behavior: requested=%v err=%v", requested, err)
	}
	for _, value := range []string{"", "True", "false", "1"} {
		if _, err := codewireDirectVolumeRequested(map[string]string{codewireKataDirectVolumeParameter: value}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("marker %q did not fail closed: %v", value, err)
		}
	}
	manager := newTestCodewireManager(t, &fakeKataDirectVolumeRuntime{})
	if err := manager.Stage("managed-volume", "/stage", "/dev/longhorn/managed-volume"); err != nil {
		t.Fatal(err)
	}
	ns := &NodeServer{directVolumes: manager}
	if _, err := ns.codewireDirectVolumeMode("managed-volume", nil); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("managed volume without marker did not fail closed: %v", err)
	}
	requested, err = ns.codewireDirectVolumeMode("standard-volume", nil)
	if err != nil || requested {
		t.Fatalf("standard volume changed behavior: requested=%v err=%v", requested, err)
	}
}

func TestCodewireDirectVolumeStatsValidation(t *testing.T) {
	for _, test := range []struct {
		name  string
		stats string
	}{
		{name: "unknown unit", stats: `{"usage":[{"total":1,"unit":0}]}`},
		{name: "signed overflow", stats: `{"usage":[{"total":` + "9223372036854775808" + `,"unit":1}]}`},
		{name: "invalid json", stats: `{`},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := newTestCodewireManager(t, &fakeKataDirectVolumeRuntime{stats: []byte(test.stats)})
			if err := manager.Stage("volume", "/stage", "/dev/longhorn/volume"); err != nil {
				t.Fatal(err)
			}
			info := codewireDirectVolumeMountInfo{VolumeType: "block", Device: "/dev/longhorn/volume", FsType: "ext4", Metadata: map[string]string{}, Options: []string{}}
			if err := manager.Publish(context.Background(), "volume", "/target", info); err != nil {
				t.Fatal(err)
			}
			if _, err := manager.Stats(context.Background(), "volume", "/target"); err == nil {
				t.Fatal("expected invalid stats error")
			}
		})
	}
	manager := newTestCodewireManager(t, &fakeKataDirectVolumeRuntime{stats: []byte(`{"usage":[{"total":1,"unit":1}]}`), err: errors.New("runtime unavailable")})
	if err := manager.Stage("volume", "/stage", "/dev/longhorn/volume"); err != nil {
		t.Fatal(err)
	}
	info := codewireDirectVolumeMountInfo{VolumeType: "block", Device: "/dev/longhorn/volume", FsType: "ext4", Metadata: map[string]string{}, Options: []string{}}
	if err := manager.Publish(context.Background(), "volume", "/target", info); err == nil {
		t.Fatal("expected runtime add error")
	}
	if _, err := manager.Stats(context.Background(), "volume", "/target"); err == nil {
		t.Fatal("expected runtime error")
	}
}

func TestHostKataRuntimeUsesHostRootAndRedactsErrors(t *testing.T) {
	var command string
	var args []string
	runtime := &hostKataRuntime{run: func(_ context.Context, gotCommand string, gotArgs ...string) ([]byte, error) {
		command = gotCommand
		args = append([]string(nil), gotArgs...)
		return nil, errors.New("plaintext-that-must-not-escape")
	}}
	err := runtime.Add(context.Background(), "/target", codewireDirectVolumeMountInfo{
		VolumeType: "block",
		Device:     "/dev/longhorn/volume",
		FsType:     "ext4",
		Metadata:   map[string]string{codewireStorageKeyURIKey: codewireStorageKeyURIPrefix + testCodewireKeyID},
		Options:    []string{},
	})
	if err == nil || strings.Contains(err.Error(), "plaintext-that-must-not-escape") {
		t.Fatalf("runtime error was not redacted: %v", err)
	}
	if command != nsMounterPath || len(args) < 4 || args[0] != "--host-root" || args[1] != kataRuntimePath || args[2] != "direct-volume" || args[3] != "add" {
		t.Fatalf("unexpected host Kata command: %q %#v", command, args)
	}
}
