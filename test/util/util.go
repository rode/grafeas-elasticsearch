package util

import (
	"context"
	"fmt"
	fake "github.com/brianvoe/gofakeit/v5"
	"github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/project_go_proto"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"log"
	"testing"
)

type Setup struct {
	Ctx context.Context
	Gc  grafeas_go_proto.GrafeasV1Beta1Client
	Pc  project_go_proto.ProjectsClient
}

func checkErrFatal(e error) {
	if e != nil {
		log.Fatalf("Failed to create test setup.\nError: %v", e)
	}
}

func NewSetup() *Setup {
	ctx := context.Background()

	projectsCC, err := grpc.DialContext(ctx, "localhost:8080", grpc.WithInsecure())
	checkErrFatal(err)

	projectsClient := project_go_proto.NewProjectsClient(projectsCC)

	grafeasCC, err := grpc.DialContext(ctx, "localhost:8080", grpc.WithInsecure())
	checkErrFatal(err)

	grafeasClient := grafeas_go_proto.NewGrafeasV1Beta1Client(grafeasCC)

	return &Setup{
		ctx,
		grafeasClient,
		projectsClient,
	}
}

func NewExpect(t *testing.T) func(actual interface{}, extra ...interface{}) Assertion {
	g := NewGomegaWithT(t)
	return g.Expect
}

func RandomProjectName() string {
	return fmt.Sprintf("projects/%s", fake.UUID())
}

func CreateProject(s *Setup, n string) (*project_go_proto.Project, error) {
	return s.Pc.CreateProject(s.Ctx, &project_go_proto.CreateProjectRequest{
		Project: &project_go_proto.Project{
			Name: n,
		},
	})
}
