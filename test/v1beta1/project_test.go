// Copyright 2021 The Rode Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1beta1_test

import (
	"github.com/grafeas/grafeas/proto/v1beta1/project_go_proto"
	. "github.com/onsi/gomega"
	"github.com/rode/grafeas-elasticsearch/test/util"
	"testing"
)

func TestProject(t *testing.T) {
	Expect := util.NewExpect(t)
	s := util.NewSetup()

	t.Run("creating a project", func(t *testing.T) {
		t.Run("should succeed with a valid name", func(t *testing.T) {
			name := util.RandomProjectName()

			p, err := util.CreateProject(s, name)
			Expect(err).ToNot(HaveOccurred())

			_, err = s.Pc.GetProject(s.Ctx, &project_go_proto.GetProjectRequest{Name: p.GetName()})
			Expect(err).ToNot(HaveOccurred())
		})
		t.Run("should fail if the project already exists", func(t *testing.T) {
			name := util.RandomProjectName()

			_, err := util.CreateProject(s, name)
			_, err = util.CreateProject(s, name)
			Expect(err).To(HaveOccurred())

		})
	})

	t.Run("listing projects", func(t *testing.T) {
		names := []string{
			"projects/foo",
			"projects/bar",
			"projects/foo-bar-123",
		}

		for _, n := range names {
			_, err := util.CreateProject(s, n)
			Expect(err).ToNot(HaveOccurred())
		}

		t.Run("filters", func(t *testing.T) {
			for _, tc := range []struct {
				name, filter     string
				expected         *[]string
				shouldErrorOccur bool
			}{
				{
					name:     "single exact name match",
					filter:   `name=="projects/foo"`,
					expected: &[]string{"projects/foo"},
				},
				{
					name:     "or exact name match",
					filter:   `name=="projects/foo"||name=="projects/bar"`,
					expected: &[]string{"projects/foo", "projects/bar"},
				},
				{
					name:     "no name match",
					filter:   `name=="projects/does-not-exist"`,
					expected: &[]string{},
				},
				{
					name:             "bad filter expression",
					filter:           `name==projects/no-quotes`,
					shouldErrorOccur: true,
				},
			} {
				// ensure parallel tests are run with correct test case
				tc := tc

				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					response, err := s.Pc.ListProjects(s.Ctx, &project_go_proto.ListProjectsRequest{Filter: tc.filter})

					if tc.shouldErrorOccur {
						Expect(err).To(HaveOccurred())
					} else {
						Expect(err).ToNot(HaveOccurred())

						Expect(len(response.Projects)).To(Equal(len(*tc.expected)))
						for _, p := range response.Projects {
							Expect(p.GetName()).To(BeElementOf(*tc.expected))
						}
					}
				})
			}
		})

		for _, n := range names {
			_, err := s.Pc.DeleteProject(s.Ctx, &project_go_proto.DeleteProjectRequest{Name: n})
			Expect(err).To(HaveOccurred())
		}
	})

	t.Run("deleting a project", func(t *testing.T) {
		t.Run("should successfully remove an existing project", func(t *testing.T) {
			name := util.RandomProjectName()

			p, err := util.CreateProject(s, name)
			Expect(err).ToNot(HaveOccurred())

			// Currently Grafeas returns an error even on successful delete.
			// This makes testing delete scenarios awkward.
			// For now we ignore response on delete, and check for error on a subsequent lookup, assuming it won't be found.
			//
			// TODO: Once https://github.com/grafeas/grafeas/pull/468 is merged and released,
			//   refactor this test to actually review delete results

			s.Pc.DeleteProject(s.Ctx, &project_go_proto.DeleteProjectRequest{Name: name})

			_, err = s.Pc.GetProject(s.Ctx, &project_go_proto.GetProjectRequest{Name: p.GetName()})
			Expect(err).To(HaveOccurred())
		})
	})
}
