package v1beta1_test

import (
	"fmt"
	fake "github.com/brianvoe/gofakeit/v5"
	"github.com/golang/protobuf/ptypes"
	"github.com/grafeas/grafeas/proto/v1beta1/attestation_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/build_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/common_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/deployment_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/package_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/provenance_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/vulnerability_go_proto"
	. "github.com/onsi/gomega"
	"github.com/rode/grafeas-elasticsearch/test/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

		_, _ = s.Gc.DeleteOccurrence(s.Ctx, &grafeas_go_proto.DeleteOccurrenceRequest{Name: o.Name})

		_, err = s.Gc.GetOccurrence(s.Ctx, &grafeas_go_proto.GetOccurrenceRequest{Name: o.GetName()})
		Expect(err).To(HaveOccurred())
		Expect(status.Code(err)).To(Equal(codes.NotFound))
	})

	t.Run("listing occurrences", func(t *testing.T) {
		// setup project specifically for listing occurrences
		listProjectName := util.RandomProjectName()

		_, err := util.CreateProject(s, listProjectName)
		Expect(err).ToNot(HaveOccurred())

		// creating different occurrences to test against
		buildOccurrence := createFakeBuildOccurrence(listProjectName)
		vulnerabilityOccurrence := createFakeVulnerabilityOccurrence(listProjectName)
		attestationOccurrence := createFakeAttestationOccurrence(listProjectName)
		deploymentOccurrence := createFakeDeploymentOccurrence(listProjectName)

		secondBuildOccurrence := createFakeBuildOccurrence(listProjectName)
		secondVulnerabilityOccurrence := createFakeVulnerabilityOccurrence(listProjectName)

		// ensure occurrences have something in common to filter against
		buildOccurrence.Resource.Uri = vulnerabilityOccurrence.Resource.Uri
		vulnerabilityOccurrence.NoteName = attestationOccurrence.NoteName
		deploymentOccurrence.Resource.Uri = secondVulnerabilityOccurrence.Resource.Uri
		secondBuildOccurrence.Resource.Uri = secondVulnerabilityOccurrence.Resource.Uri

		// create
		batchResponse, err := s.Gc.BatchCreateOccurrences(s.Ctx, &grafeas_go_proto.BatchCreateOccurrencesRequest{
			Parent: listProjectName,
			Occurrences: []*grafeas_go_proto.Occurrence{
				buildOccurrence,
				vulnerabilityOccurrence,
				attestationOccurrence,
				deploymentOccurrence,
				secondBuildOccurrence,
				secondVulnerabilityOccurrence,
			},
		})
		Expect(err).ToNot(HaveOccurred())

		// reassign pointer values for test occurrences, since the created occurrences will have a new `Name` field that
		// will need to be included in our assertions
		buildOccurrence = batchResponse.Occurrences[0]
		vulnerabilityOccurrence = batchResponse.Occurrences[1]
		attestationOccurrence = batchResponse.Occurrences[2]
		deploymentOccurrence = batchResponse.Occurrences[3]

		secondBuildOccurrence = batchResponse.Occurrences[4]
		secondVulnerabilityOccurrence = batchResponse.Occurrences[5]

		t.Run("should be successful", func(t *testing.T) {
			res, err := s.Gc.ListOccurrences(s.Ctx, &grafeas_go_proto.ListOccurrencesRequest{
				Parent: listProjectName,
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(res.Occurrences).To(HaveLen(6))
		})

		t.Run("filters", func(t *testing.T) {
			for _, tc := range []struct {
				name, filter string
				expected     []*grafeas_go_proto.Occurrence
				expectError  bool
			}{
				{
					name:   "match resource uri",
					filter: fmt.Sprintf(`"resource.uri"=="%s"`, buildOccurrence.Resource.Uri),
					expected: []*grafeas_go_proto.Occurrence{
						buildOccurrence,
						vulnerabilityOccurrence,
					},
				},
				{
					name:   "match note name",
					filter: fmt.Sprintf(`"noteName"=="%s"`, vulnerabilityOccurrence.NoteName),
					expected: []*grafeas_go_proto.Occurrence{
						vulnerabilityOccurrence,
						attestationOccurrence,
					},
				},
				{
					name:   "match all occurrence types via OR",
					filter: `"kind"=="VULNERABILITY" || "kind"=="ATTESTATION" || "kind"=="BUILD" || "kind"=="DEPLOYMENT"`,
					expected: []*grafeas_go_proto.Occurrence{
						vulnerabilityOccurrence,
						attestationOccurrence,
						buildOccurrence,
						deploymentOccurrence,
						secondBuildOccurrence,
						secondVulnerabilityOccurrence,
					},
				},
				{
					name:   "match all occurrence types via OR and !=",
					filter: fmt.Sprintf(`"kind"!="VULNERABILITY" || "resource.uri"=="%s"`, vulnerabilityOccurrence.Resource.Uri),
					expected: []*grafeas_go_proto.Occurrence{
						vulnerabilityOccurrence,
						attestationOccurrence,
						buildOccurrence,
						deploymentOccurrence,
						secondBuildOccurrence,
					},
				},
				{
					name:     "match nothing",
					filter:   fmt.Sprintf(`"kind"=="VULNERABILITY" && "resource.uri" == "%s"`, attestationOccurrence.Resource.Uri),
					expected: []*grafeas_go_proto.Occurrence{},
				},
				{
					name:   "match non vulnerability occurrences via !=",
					filter: `"kind"!="VULNERABILITY"`,
					expected: []*grafeas_go_proto.Occurrence{
						buildOccurrence,
						attestationOccurrence,
						deploymentOccurrence,
						secondBuildOccurrence,
					},
				},
				{
					name:   "match second vuln occurrence via && and !=",
					filter: fmt.Sprintf(`"resource.uri" == "%s" && ("kind" != "BUILD" && "kind" != "DEPLOYMENT")`, deploymentOccurrence.Resource.Uri),
					expected: []*grafeas_go_proto.Occurrence{
						secondVulnerabilityOccurrence,
					},
				},
				{
					name:        "bad filter",
					filter:      "lol",
					expectError: true,
				},
			} {
				// ensure parallel tests are run with correct test case
				tc := tc

				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					res, err := s.Gc.ListOccurrences(s.Ctx, &grafeas_go_proto.ListOccurrencesRequest{
						Parent: listProjectName,
						Filter: tc.filter,
					})

					if tc.expectError {
						Expect(err).To(HaveOccurred())
					} else {
						Expect(err).ToNot(HaveOccurred())
						Expect(res.Occurrences).To(HaveLen(len(tc.expected)))
						Expect(tc.expected).To(ConsistOf(res.Occurrences))
					}
				})
			}
		})

		t.Run("should successfully use the filter", func(t *testing.T) {
			expectedResourceUri := buildOccurrence.Resource.Uri
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

func createFakeDeploymentOccurrence(projectName string) *grafeas_go_proto.Occurrence {
	return &grafeas_go_proto.Occurrence{
		Resource: &grafeas_go_proto.Resource{
			Uri: fake.URL(),
		},
		NoteName: util.RandomNoteName(projectName),
		Kind:     common_go_proto.NoteKind_DEPLOYMENT,
		Details: &grafeas_go_proto.Occurrence_Deployment{
			Deployment: &deployment_go_proto.Details{
				Deployment: &deployment_go_proto.Deployment{
					UserEmail:   fake.Email(),
					DeployTime:  ptypes.TimestampNow(),
					Config:      fake.LoremIpsumSentence(fake.Number(2, 5)),
					Address:     fake.DomainName(),
					ResourceUri: []string{fake.URL()},
					Platform:    deployment_go_proto.Deployment_CUSTOM,
				},
			},
		},
	}
}
