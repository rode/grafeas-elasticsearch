package v1beta1_test

import (
	"fmt"
	fake "github.com/brianvoe/gofakeit/v5"
	"github.com/grafeas/grafeas/proto/v1beta1/build_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/provenance_go_proto"
	"github.com/liatrio/grafeas-elasticsearch/test/util"
	. "github.com/onsi/gomega"
	"testing"
)

func TestOccurrence(t *testing.T) {
	Expect := util.NewExpect(t)
	s := util.NewSetup()

	t.Run("creating an occurrence", func(t *testing.T) {
		t.Parallel()

		projectName := util.RandomProjectName()

		_, err := util.CreateProject(s, projectName)
		Expect(err).ToNot(HaveOccurred())

		t.Run("should be successful", func(t *testing.T) {
			o, err := s.Gc.CreateOccurrence(s.Ctx, &grafeas_go_proto.CreateOccurrenceRequest{
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

			_, err = s.Gc.GetOccurrence(s.Ctx, &grafeas_go_proto.GetOccurrenceRequest{Name: o.GetName()})
			Expect(err).ToNot(HaveOccurred())
		})
	})
}
