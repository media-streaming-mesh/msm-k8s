package stream_api

import (
	"context"
	"fmt"
	mediastreamsv1 "github.com/media-streaming-mesh/msm-k8s/api/v1"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"log"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
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

func (s *StreamAPI) Create(data *mediastreamsv1.Streamdata) error {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err := s.crdClient.Create(ctx, data)
	return err
}

func (s *StreamAPI) Update(data *mediastreamsv1.Streamdata) error {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err := s.crdClient.Update(ctx, data)
	return err
}
