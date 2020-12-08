package v1beta1_test

import (
	"context"
	"fmt"
	fake "github.com/brianvoe/gofakeit/v5"
	"github.com/grafeas/grafeas/proto/v1beta1/build_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/project_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/provenance_go_proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"log"
)

type setup struct {
	ctx context.Context
	gc  grafeas_go_proto.GrafeasV1Beta1Client
	pc  project_go_proto.ProjectsClient
}

func newSetup() (*setup, error) {
	ctx := context.Background()

	projectsCC, err := grpc.DialContext(ctx, "localhost:8080", grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	projectsClient := project_go_proto.NewProjectsClient(projectsCC)

	grafeasCC, err := grpc.DialContext(ctx, "localhost:8080", grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	grafeasClient := grafeas_go_proto.NewGrafeasV1Beta1Client(grafeasCC)

	return &setup{
		ctx,
		grafeasClient,
		projectsClient,
	}, nil
}

var _ = Describe("Grafeas Elasticsearch", func() {
	var (
		err error
		s   *setup
	)

	fake.Seed(0)

	BeforeEach(func() {
		s, err = newSetup()

		if err != nil {
			log.Fatalf("Failed to create test setup.\nError: %v", err)
		}
	})

	AfterEach(func() {
		err = nil
		s = nil
	})

	Describe("Projects", func() {
		Context("creating a project", func() {
			When("using a valid name", func() {
				It("should be successful", func() {
					name := randomProjectName()

					By("creating the project")

					p, err := s.pc.CreateProject(s.ctx, &project_go_proto.CreateProjectRequest{Project: &project_go_proto.Project{Name: name}})
					Expect(err).ToNot(HaveOccurred())

					By("retrieving the project")

					_, err = s.pc.GetProject(s.ctx, &project_go_proto.GetProjectRequest{Name: p.GetName()})
					Expect(err).ToNot(HaveOccurred())
				})
			})
		})

		//Context("deleting a project", func() {
		//	When("it exists", func() {
		//		It("should be successfully removed", func() {
		//			name := randomProjectName()
		//
		//			By("creating the project")
		//
		//			p, err := s.pc.CreateProject(s.ctx, &project_go_proto.CreateProjectRequest{Project: &project_go_proto.Project{Name: name}})
		//			Expect(err).ToNot(HaveOccurred())
		//
		//			By("deleting the project")
		//
		//			_, err = s.pc.DeleteProject(s.ctx, &project_go_proto.DeleteProjectRequest{Name: p.GetName()})
		//			Expect(err).ToNot(HaveOccurred())
		//		})
		//	})
		//})
	})

	Describe("Occurrences", func() {
		Context("creating an occurrence", func() {
			var projectName string

			BeforeEach(func() {
				projectName = randomProjectName()

				By("creating a project")

				_, err = s.pc.CreateProject(s.ctx, &project_go_proto.CreateProjectRequest{Project: &project_go_proto.Project{Name: projectName}})
				Expect(err).ToNot(HaveOccurred())
			})

			It("should be successful", func() {
				By("creating the occurrence")

				o, err := s.gc.CreateOccurrence(s.ctx, &grafeas_go_proto.CreateOccurrenceRequest{
					Parent: projectName,
					Occurrence: &grafeas_go_proto.Occurrence{
						Details: &grafeas_go_proto.Occurrence_Build{Build: &build_go_proto.Details{
							Provenance: &provenance_go_proto.BuildProvenance{
								Id: fake.UUID(),
							},
						}},
						NoteName: fmt.Sprintf("%s/notes/%s", projectName, fake.AppName()),
						Resource: &grafeas_go_proto.Resource{
							Uri: fake.URL(),
						},
					},
				})
				Expect(err).ToNot(HaveOccurred())

				By("retrieving the occurrence")

				_, err = s.gc.GetOccurrence(s.ctx, &grafeas_go_proto.GetOccurrenceRequest{Name: o.GetName()})
				Expect(err).ToNot(HaveOccurred())
			})
		})
	})
})

func randomProjectName() string {
	return fmt.Sprintf("projects/%s", fake.UUID())
}
