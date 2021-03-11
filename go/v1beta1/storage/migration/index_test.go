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

package migration

import (
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("index manager", func() {
	var (
		indexManager *EsIndexManager
		projectId    string
	)

	BeforeEach(func() {
		projectId = fake.LetterN(10)
		indexManager = NewEsIndexManager(logger, nil)

		populateIndexMappings(indexManager)
	})

	Context("alias name functions", func() {
		Describe("ProjectsAlias", func() {
			It("should return the project alias", func() {
				Expect(indexManager.ProjectsAlias()).To(Equal(createIndexOrAliasName("projects")))
			})
		})

		Describe("OccurrencesAlias", func() {
			It("should construct the alias for the given project's occurrences index", func() {
				Expect(indexManager.OccurrencesAlias(projectId)).To(Equal(createIndexOrAliasName(projectId, "occurrences")))
			})
		})

		Describe("NotesAlias", func() {
			It("should construct the alias for the given project's notes index", func() {
				Expect(indexManager.NotesAlias(projectId)).To(Equal(createIndexOrAliasName(projectId, "notes")))
			})
		})
	})

	Context("index name functions", func() {
		Describe("ProjectsIndex", func() {
			It("should return the versioned projects index", func() {
				expectedIndexName := createIndexOrAliasName(indexManager.projectMapping.Version, "projects")

				Expect(indexManager.ProjectsIndex()).To(Equal(expectedIndexName))
			})
		})

		Describe("OccurrencesIndex", func() {
			It("should return the versioned occurrences index", func() {
				expectedIndexName := createIndexOrAliasName(indexManager.occurrenceMapping.Version, projectId, "occurrences")

				Expect(indexManager.OccurrencesIndex(projectId)).To(Equal(expectedIndexName))
			})
		})

		Describe("NotesIndex", func() {
			It("should return the versioned notes index", func() {
				expectedIndexName := createIndexOrAliasName(indexManager.noteMapping.Version, projectId, "notes")

				Expect(indexManager.NotesIndex(projectId)).To(Equal(expectedIndexName))
			})
		})
	})
})

func populateIndexMappings(indexManager *EsIndexManager) {
	indexManager.projectMapping = &VersionedMapping{
		Mappings: fake.Map(),
		Version: fake.LetterN(5),
	}

	indexManager.occurrenceMapping = &VersionedMapping{
		Mappings: fake.Map(),
		Version: fake.LetterN(5),
	}

	indexManager.noteMapping = &VersionedMapping{
		Mappings: fake.Map(),
		Version: fake.LetterN(5),
	}
}

func createIndexOrAliasName(parts ...string) string {
	withPrefix := append([]string{"grafeas"}, parts...)

	return strings.Join(withPrefix, "-")
}