package v1beta1_test

import (
	"context"
	"fmt"
	"github.com/brianvoe/gofakeit/v5"
	"github.com/grafeas/grafeas/proto/v1beta1/project_go_proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"log"
	"strings"
)

type setup struct {
	ctx            context.Context
	projectsClient project_go_proto.ProjectsClient
}

func newSetup() (*setup, error) {
	ctx := context.Background()

	projectsCC, err := grpc.DialContext(ctx, "localhost:8080", grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	projectsClient := project_go_proto.NewProjectsClient(projectsCC)

	return &setup{
		ctx,
		projectsClient,
	}, nil
}

var _ = Describe("Integration", func() {
	var (
		err error
		s   *setup
	)

	gofakeit.Seed(0)

	BeforeEach(func() {
		s, err = newSetup()

		if err != nil {
			log.Fatalf("Failed to create integration test setup.\nError: %v", err)
		}
	})

	Describe("Creating a project", func() {
		When("a valid name is used", func() {
			It("should be successful", func() {
				name := randomProjectName()

				project, err := s.projectsClient.CreateProject(s.ctx, &project_go_proto.CreateProjectRequest{Project: &project_go_proto.Project{Name: name}})

				Expect(err).ToNot(HaveOccurred())

				_, err = s.projectsClient.GetProject(s.ctx, &project_go_proto.GetProjectRequest{Name: project.GetName()})
				Expect(err).ToNot(HaveOccurred())
			})
		})
	})
})

func randomProjectName() string {
	return fmt.Sprintf("projects/%s", strings.ToLower(gofakeit.AppName()))
}
