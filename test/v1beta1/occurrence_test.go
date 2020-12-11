package v1beta1_test

import (
	fake "github.com/brianvoe/gofakeit/v5"
	"github.com/grafeas/grafeas/proto/v1beta1/build_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/package_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/provenance_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/vulnerability_go_proto"
	"github.com/liatrio/grafeas-elasticsearch/test/util"
	. "github.com/onsi/gomega"
	"testing"
)

func TestOccurrence(t *testing.T) {
	Expect := util.NewExpect(t)
	s := util.NewSetup()

	// setup project for occurrences
	projectName := util.RandomProjectName()

	_, err := util.CreateProject(s, projectName)
	Expect(err).ToNot(HaveOccurred())

	t.Run("creating an occurrence", func(t *testing.T) {
		t.Run("should be successful", func(t *testing.T) {
			o, err := s.Gc.CreateOccurrence(s.Ctx, &grafeas_go_proto.CreateOccurrenceRequest{
				Parent: projectName,
				Occurrence: &grafeas_go_proto.Occurrence{
					Details: &grafeas_go_proto.Occurrence_Build{
						Build: &build_go_proto.Details{
							Provenance: &provenance_go_proto.BuildProvenance{
								Id: fake.UUID(),
							},
						}},
					NoteName: util.RandomNoteName(projectName),
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

	t.Run("batch creating occurrences", func(t *testing.T) {
		bo, err := s.Gc.BatchCreateOccurrences(s.Ctx, &grafeas_go_proto.BatchCreateOccurrencesRequest{
			Parent: projectName,
			Occurrences: []*grafeas_go_proto.Occurrence{
				{
					Details: &grafeas_go_proto.Occurrence_Build{
						Build: &build_go_proto.Details{
							Provenance: &provenance_go_proto.BuildProvenance{
								Id: fake.UUID(),
							},
						}},
					NoteName: util.RandomNoteName(projectName),
					Resource: &grafeas_go_proto.Resource{
						Uri: fake.URL(),
					},
				},
				{
					Details: &grafeas_go_proto.Occurrence_Vulnerability{
						Vulnerability: &vulnerability_go_proto.Details{
							PackageIssue: []*vulnerability_go_proto.PackageIssue{
								{
									AffectedLocation: &vulnerability_go_proto.VulnerabilityLocation{
										CpeUri:  fake.URL(),
										Package: fake.AppName(),
										Version: &package_go_proto.Version{
											Name: fake.AppVersion(),
											Kind: package_go_proto.Version_NORMAL,
										},
									},
								},
							},
						},
					},
					NoteName: util.RandomNoteName(projectName),
					Resource: &grafeas_go_proto.Resource{
						Uri: fake.URL(),
					},
				},
			},
		})

		Expect(err).ToNot(HaveOccurred())

		for _, o := range bo.Occurrences {
			_, err = s.Gc.GetOccurrence(s.Ctx, &grafeas_go_proto.GetOccurrenceRequest{Name: o.GetName()})
			Expect(err).ToNot(HaveOccurred())
		}
	})

	t.Run("deleting an occurrence", func(t *testing.T) {
		o, err := s.Gc.CreateOccurrence(s.Ctx, &grafeas_go_proto.CreateOccurrenceRequest{
			Parent: projectName,
			Occurrence: &grafeas_go_proto.Occurrence{
				Details: &grafeas_go_proto.Occurrence_Build{
					Build: &build_go_proto.Details{
						Provenance: &provenance_go_proto.BuildProvenance{
							Id: fake.UUID(),
						},
					}},
				NoteName: util.RandomNoteName(projectName),
				Resource: &grafeas_go_proto.Resource{
					Uri: fake.URL(),
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		// Currently Grafeas returns an error even on successful delete.
		// This makes testing delete scenarios awkward.
		// For now we ignore response on delete, and check for error on a subsequent lookup, assuming it won't be found.
		//
		// TODO: Once a new version of Grafeas is released that contains this fix:
		//  https://github.com/grafeas/grafeas/pull/456
		//  This should be updated to actually review delete results

		s.Gc.DeleteOccurrence(s.Ctx, &grafeas_go_proto.DeleteOccurrenceRequest{Name: o.Name})

		_, err = s.Gc.GetOccurrence(s.Ctx, &grafeas_go_proto.GetOccurrenceRequest{Name: o.GetName()})
		Expect(err).To(HaveOccurred())
	})

	t.Run("listing occurrences", func(t *testing.T) {
		t.Skipf("TODO")
	})
}
