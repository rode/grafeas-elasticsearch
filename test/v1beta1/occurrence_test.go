package v1beta1_test

import (
	"fmt"
	fake "github.com/brianvoe/gofakeit/v5"
	"github.com/grafeas/grafeas/proto/v1beta1/attestation_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/build_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/common_go_proto"
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
				Parent:     projectName,
				Occurrence: createFakeBuildOccurrence(projectName),
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
				createFakeBuildOccurrence(projectName),
				createFakeVulnerabilityOccurrence(projectName),
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
			Parent:     projectName,
			Occurrence: createFakeBuildOccurrence(projectName),
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
		// setup project specifically for listing occurrences
		listProjectName := util.RandomProjectName()

		_, err := util.CreateProject(s, listProjectName)
		Expect(err).ToNot(HaveOccurred())

		// creating three different occurrences to test against
		occurrences := []*grafeas_go_proto.Occurrence{
			createFakeBuildOccurrence(listProjectName),
			createFakeVulnerabilityOccurrence(listProjectName),
			createFakeAttestationOccurrence(listProjectName),
		}

		// ensure the first two occurrences have something in common to filter against
		occurrences[0].Resource.Uri = occurrences[1].Resource.Uri

		// create
		_, err = s.Gc.BatchCreateOccurrences(s.Ctx, &grafeas_go_proto.BatchCreateOccurrencesRequest{
			Parent:      listProjectName,
			Occurrences: occurrences,
		})
		Expect(err).ToNot(HaveOccurred())

		t.Run("should be successful", func(t *testing.T) {
			res, err := s.Gc.ListOccurrences(s.Ctx, &grafeas_go_proto.ListOccurrencesRequest{
				Parent: listProjectName,
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(res.Occurrences).To(HaveLen(len(occurrences)))
		})

		t.Run("should successfully use the filter", func(t *testing.T) {
			expectedResourceUri := occurrences[0].Resource.Uri
			res, err := s.Gc.ListOccurrences(s.Ctx, &grafeas_go_proto.ListOccurrencesRequest{
				Parent: listProjectName,
				Filter: fmt.Sprintf(`"resource.uri"=="%s"`, expectedResourceUri),
			})
			Expect(err).ToNot(HaveOccurred())

			for _, o := range res.Occurrences {
				Expect(o.Resource.Uri).To(Equal(expectedResourceUri))
			}
		})
	})
}

func createFakeBuildOccurrence(projectName string) *grafeas_go_proto.Occurrence {
	return &grafeas_go_proto.Occurrence{
		Resource: &grafeas_go_proto.Resource{
			Uri: fake.URL(),
		},
		NoteName: util.RandomNoteName(projectName),
		Kind:     common_go_proto.NoteKind_BUILD,
		Details: &grafeas_go_proto.Occurrence_Build{
			Build: &build_go_proto.Details{
				Provenance: &provenance_go_proto.BuildProvenance{
					Id: fake.UUID(),
				},
			},
		},
	}
}

func createFakeVulnerabilityOccurrence(projectName string) *grafeas_go_proto.Occurrence {
	return &grafeas_go_proto.Occurrence{
		Resource: &grafeas_go_proto.Resource{
			Uri: fake.URL(),
		},
		NoteName: util.RandomNoteName(projectName),
		Kind:     common_go_proto.NoteKind_VULNERABILITY,
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
	}
}

func createFakeAttestationOccurrence(projectName string) *grafeas_go_proto.Occurrence {
	return &grafeas_go_proto.Occurrence{
		Resource: &grafeas_go_proto.Resource{
			Uri: fake.URL(),
		},
		NoteName: util.RandomNoteName(projectName),
		Kind:     common_go_proto.NoteKind_ATTESTATION,
		Details: &grafeas_go_proto.Occurrence_Attestation{
			Attestation: &attestation_go_proto.Details{
				Attestation: &attestation_go_proto.Attestation{
					Signature: &attestation_go_proto.Attestation_GenericSignedAttestation{
						GenericSignedAttestation: &attestation_go_proto.GenericSignedAttestation{
							ContentType:       attestation_go_proto.GenericSignedAttestation_CONTENT_TYPE_UNSPECIFIED,
							SerializedPayload: []byte(fake.LetterN(10)),
							Signatures: []*common_go_proto.Signature{
								{
									Signature:   []byte(fake.LetterN(10)),
									PublicKeyId: fake.LetterN(10),
								},
							},
						},
					},
				},
			},
		},
	}
}
