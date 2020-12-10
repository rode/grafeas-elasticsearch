package v1beta1_test

import (
	"context"
	"fmt"
	fake "github.com/brianvoe/gofakeit/v5"
	"github.com/grafeas/grafeas/proto/v1beta1/build_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/project_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/provenance_go_proto"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"log"
	"testing"
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

func TestGrafeasElasticsearch(t *testing.T) {
	if testing.Short() {
		t.Skipf("Test run with -short flag, skipping.")
	}

	fake.Seed(0)

	g := NewGomegaWithT(t)
	Expect := g.Expect

	s, err := newSetup()
	if err != nil {
		log.Fatalf("Failed to create test setup.\nError: %v", err)
	}

	t.Run("projects", func(t *testing.T) {
		t.Run("creating a project", func(t *testing.T) {
			t.Run("should succeed with a valid name", func(t *testing.T) {
				name := randomProjectName()

				p, err := createProject(s, name)
				Expect(err).ToNot(HaveOccurred())

				_, err = s.pc.GetProject(s.ctx, &project_go_proto.GetProjectRequest{Name: p.GetName()})
				Expect(err).ToNot(HaveOccurred())
			})
		})

		t.Run("listing projects", func(t *testing.T) {
			type test struct {
				name, filter string
				expected     []string
			}

			names := []string{
				"projects/foo",
				"projects/bar",
				"projects/foo-bar-123",
			}

			for _, n := range names {
				_, err = createProject(s, n)
				Expect(err).ToNot(HaveOccurred())
			}

			t.Run("filters", func(t *testing.T) {
				for _, tc := range []test{
					{
						name:     "single exact name match",
						filter:   "name==\"projects/foo\"",
						expected: []string{"projects/foo"},
					},
					{
						name:     "or exact name match",
						filter:   "name==\"projects/foo\"||name==\"projects/bar\"",
						expected: []string{"projects/foo", "projects/bar"},
					},
				} {
					tc := tc

					t.Run(tc.name, func(t *testing.T) {
						t.Parallel()

						response, err := s.pc.ListProjects(s.ctx, &project_go_proto.ListProjectsRequest{Filter: tc.filter})
						Expect(err).ToNot(HaveOccurred())

						Expect(len(response.Projects)).To(Equal(len(tc.expected)))
						for _, p := range response.Projects {
							Expect(p.GetName()).To(BeElementOf(tc.expected))
						}
					})
				}
			})

			for _, n := range names {
				_, err = s.pc.DeleteProject(s.ctx, &project_go_proto.DeleteProjectRequest{Name: n})
				Expect(err).To(HaveOccurred())
			}
		})

		t.Run("deleting a project", func(t *testing.T) {
			t.Run("should successfully remove an existing project", func(t *testing.T) {
				name := randomProjectName()

				p, err := createProject(s, name)
				Expect(err).ToNot(HaveOccurred())

				// Currently Grafeas returns an error even on successful delete.
				// This makes testing delete scenarios awkward.
				// For now we ignore response on delete, and check for error on a subsequent lookup, assuming it won't be found.
				//
				// TODO: Once https://github.com/grafeas/grafeas/pull/468 is merged and released,
				//   refactor this test to actual review delete results and expect

				s.pc.DeleteProject(s.ctx, &project_go_proto.DeleteProjectRequest{Name: name})

				_, err = s.pc.GetProject(s.ctx, &project_go_proto.GetProjectRequest{Name: p.GetName()})
				Expect(err).To(HaveOccurred())
			})
		})
	})

	t.Run("occurrences", func(t *testing.T) {
		t.Run("creating an occurrence", func(t *testing.T) {
			projectName := randomProjectName()

			_, err = createProject(s, projectName)
			Expect(err).ToNot(HaveOccurred())

			t.Run("should be successful", func(t *testing.T) {
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

				_, err = s.gc.GetOccurrence(s.ctx, &grafeas_go_proto.GetOccurrenceRequest{Name: o.GetName()})
				Expect(err).ToNot(HaveOccurred())
			})
		})
	})
}

func randomProjectName() string {
	return fmt.Sprintf("projects/%s", fake.UUID())
}

func createProject(s *setup, n string) (*project_go_proto.Project, error) {
	return s.pc.CreateProject(s.ctx, &project_go_proto.CreateProjectRequest{
		Project: &project_go_proto.Project{
			Name: n,
		},
	})
}
