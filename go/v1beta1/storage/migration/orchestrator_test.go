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
	"context"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Orchestrator", func() {
	var (
		orchestrator *MigrationOrchestrator
		migrator     *fakeMigrator
		ctx          context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		migrator = &fakeMigrator{
			migrations:    []*Migration{},
			migrationsRan: []*Migration{},
		}
		orchestrator = NewMigrationOrchestrator(logger, migrator)
	})

	Describe("RunMigrations", func() {
		Describe("no migrations to run", func() {
			It("should not return an error", func() {
				Expect(orchestrator.RunMigrations(ctx)).To(BeNil())
			})
		})

		Describe("error occurs while getting migrations to run", func() {
			var actualError error

			BeforeEach(func() {
				migrator.getMigrationsError = fmt.Errorf(fake.Word())
				actualError = orchestrator.RunMigrations(ctx)
			})

			It("should return the error", func() {
				Expect(actualError).NotTo(BeNil())
				Expect(actualError).To(MatchError(migrator.getMigrationsError))
			})

			It("should not have tried to run any migrations", func() {
				Expect(migrator.migrationsRan).To(HaveLen(0))
			})
		})

		Describe("there are migrations to run", func() {
			BeforeEach(func() {
				migrator.migrations = []*Migration{
					createFakeMigration(),
					createFakeMigration(),
					createFakeMigration(),
				}
			})

			It("should run all migrations", func() {
				Expect(orchestrator.RunMigrations(ctx)).To(BeNil())
				Expect(migrator.migrationsRan).To(ConsistOf(migrator.migrations))
			})

			It("should return the error if a migration fails and not try to run any additional migrations", func() {
				expectedErr := fmt.Errorf(fake.Word())
				migrator.migrateError = expectedErr

				actualErr := orchestrator.RunMigrations(ctx)

				Expect(actualErr).To(MatchError(expectedErr))
				Expect(migrator.migrationsRan).To(HaveLen(1))
			})
		})
	})
})

type fakeMigrator struct {
	migrations         []*Migration
	getMigrationsError error
	migrateError       error

	migrationsRan []*Migration
}

func (fm *fakeMigrator) GetMigrations(_ context.Context) ([]*Migration, error) {
	if fm.getMigrationsError != nil {
		return nil, fm.getMigrationsError
	}

	return fm.migrations, nil
}

func (fm *fakeMigrator) Migrate(_ context.Context, migration *Migration) error {
	fm.migrationsRan = append(fm.migrationsRan, migration)

	if fm.migrateError != nil {
		return fm.migrateError
	}

	return nil
}

func createFakeMigration() *Migration {
	return &Migration{
		Index:        fake.Word(),
		Alias:        fake.Word(),
		DocumentKind: fake.Word(),
	}
}