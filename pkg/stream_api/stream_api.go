package stream_api

import (
	"context"
	"fmt"
	mediastreamsv1 "github.com/media-streaming-mesh/msm-k8s/api/v1"
	"github.com/media-streaming-mesh/msm-k8s/pkg/model"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"log"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
	"time"
)

const (
	CRDPlural   string = "streamdata"
	CRDGroup    string = "mediastreams.media-streaming-mesh.io"
	CRDVersion  string = "v1"
	FullCRDName string = CRDPlural + "." + CRDGroup
)

type StreamAPI struct {
	crdClient client.WithWatch
	logger    *logrus.Logger
}

func NewStreamAPI(logger *logrus.Logger) *StreamAPI {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		log.Fatal(err)
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(mediastreamsv1.AddToScheme(scheme))

	crdClient, err := client.NewWithWatch(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		log.Fatal(err)
	}

	return &StreamAPI{
		crdClient: crdClient,
		logger:    logger,
	}
}

func (s *StreamAPI) log(format string, args ...interface{}) {
	s.logger.Infof("[Stream API] " + fmt.Sprintf(format, args...))
}

func (s *StreamAPI) logError(format string, args ...interface{}) {
	s.logger.Errorf("[Stream API] " + fmt.Sprintf(format, args...))
}

func (s *StreamAPI) List() (*mediastreamsv1.StreamdataList, error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	result := &mediastreamsv1.StreamdataList{}

	err := s.crdClient.List(ctx, result)
	return result, err
}

func (s *StreamAPI) Get(name string) (*mediastreamsv1.Streamdata, error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	result := &mediastreamsv1.Streamdata{}

	err := s.crdClient.Get(ctx, client.ObjectKey{Namespace: "default", Name: name}, result)
	return result, err
}

// TODO: use mediastreamsv1.Streamdata when refactor msm-cp and msm-nc to mediastreamsv1.Streamdata
func (s *StreamAPI) Create(data model.StreamData) error {
	crdData := s.ModelObjToCRDObj(data)
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err := s.crdClient.Create(ctx, crdData)
	return err
}

// TODO: use mediastreamsv1.Streamdata when refactor msm-cp and msm-nc to mediastreamsv1.Streamdata
func (s *StreamAPI) Update(data model.StreamData) error {
	crdData := s.ModelObjToCRDObj(data)
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err := s.crdClient.Update(ctx, crdData)
	return err
}

// TODO: use mediastreamsv1.Streamdata when refactor msm-cp and msm-nc to mediastreamsv1.Streamdata
func (s *StreamAPI) Delete(data model.StreamData) error {
	crdData, err := s.Get(s.getCRDName(data))
	if err != nil {
		return err
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = s.crdClient.Delete(ctx, crdData)
	return err
}

// TODO: use mediastreamsv1.Streamdata when refactor msm-cp and msm-nc to mediastreamsv1.Streamdata
func (s *StreamAPI) WatchStreams(dataChan chan<- model.StreamData) {
	s.log("Start WATCH for CRD streamData")
	result := &mediastreamsv1.StreamdataList{}
	watchInterface, err := s.crdClient.Watch(context.Background(), result)
	if err != nil {
		s.log("Failed watch CRD streamData with error %v", err)
	}
	defer watchInterface.Stop()

	select {
	case event := <-watchInterface.ResultChan():
		s.log("Watch event %v result %v", event.Type, event.Object)
		crdData := event.Object.(*mediastreamsv1.Streamdata)

		//dataChan <- s.crObjToModelObj(crdData)
		s.log("crd object %v", crdData)

		if event.Type == watch.Added {
			s.log("Received Add event")
		}
	}
	s.log("End WATCH for CRD streamData")
}

func (s *StreamAPI) CRDObjToModelObj(crdData *mediastreamsv1.Streamdata) model.StreamData {
	var streamState model.StreamState

	if strings.ToLower(crdData.Spec.StreamState) == "create" {
		streamState = model.Create
	} else if strings.ToLower(crdData.Spec.StreamState) == "play" {
		streamState = model.Play
	} else if strings.ToLower(crdData.Spec.StreamState) == "teardown" {
		streamState = model.Teardown
	}

	return model.StreamData{
		StubIp:      crdData.Spec.StubIp,
		ServerIp:    crdData.Spec.ServerIp,
		ClientIp:    crdData.Spec.ClientIp,
		ClientPorts: []uint32{uint32(crdData.Spec.ClientPort)},
		ServerPorts: []uint32{uint32(crdData.Spec.ServerPort)},
		StreamState: streamState,
	}
}

func (s *StreamAPI) ModelObjToCRDObj(data model.StreamData) *mediastreamsv1.Streamdata {
	return &mediastreamsv1.Streamdata{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Streamdata",
			APIVersion: CRDGroup + "/" + CRDVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      s.getCRDName(data),
			Namespace: "default",
		},
		Spec: mediastreamsv1.StreamdataSpec{
			StubIp:     data.StubIp,
			ServerIp:   data.ServerIp,
			ClientIp:   data.ClientIp,
			ServerPort: int(data.ServerPorts[0]),
			ClientPort: int(data.ClientPorts[0]),
			NodeID:     "",
		},
		Status: mediastreamsv1.StreamdataStatus{
			Status:       "PENDING",
			Reason:       "SETUP FROM APP",
			StreamStatus: "CREATE",
		},
	}
}

// TODO: add ports
func (s *StreamAPI) getCRDName(data model.StreamData) string {
	return "streamdata" + "-" + data.ServerIp + "-" + data.ClientIp
}
