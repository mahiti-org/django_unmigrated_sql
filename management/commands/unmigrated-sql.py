from django.core.management.base import BaseCommand, CommandError
from django.db import connections
from django.db.migrations.executor import MigrationExecutor
from django.db.migrations.loader import MigrationLoader
from django.utils import timezone

class Command(BaseCommand):
    help = 'Executes all unmigrated migrations SQL in the given app, or all apps if no app is specified, using the default database (ignores errors, no transaction, marks as fake)'

    def add_arguments(self, parser):
        parser.add_argument(
            'app_label',
            nargs='?',
            type=str,
            help='App label of the application containing the migrations (e.g., "myapp"). If omitted, all apps are processed.',
        )

    def handle(self, *args, **options):
        app_label = options.get('app_label')

        # Use the default database connection
        default_connection = connections['default']
        db_cursor = default_connection.cursor()

        # Load migration graph
        loader = MigrationLoader(connection=default_connection)
        executor = MigrationExecutor(connection=default_connection)

        available_migrations = loader.disk_migrations or {}
        applied_migrations = loader.applied_migrations or set()

        if app_label:
            app_labels = [app_label]
        else:
            app_labels = sorted(set(label for (label, name) in available_migrations.keys()))

        found_unmigrated = False
        for label in app_labels:
            app_migrations = [name for (l, name) in available_migrations.keys() if l == label]
            if not app_migrations:
                if app_label:
                    raise CommandError(f"No migrations found for app '{label}'.")
                continue
            unmigrated = [name for name in sorted(app_migrations) if (label, name) not in applied_migrations]
            if not unmigrated:
                if app_label:
                    self.stdout.write(f"All migrations for '{label}' are already applied.")
                continue
            found_unmigrated = True
            self.stdout.write(f"Executing SQL for unmigrated migrations in '{label}' using default database (errors ignored, no transaction):")
            for migration_name in unmigrated:
                migration = available_migrations[(label, migration_name)]
                self.stdout.write(f"\n-- Executing SQL for {label}.{migration_name} --")

                previous_migration_name = self._get_previous_migration(loader, label, migration_name)
                from_state = loader.project_state((label, previous_migration_name)) if previous_migration_name else loader.project_state()
                to_state = loader.project_state((label, migration_name))

                with default_connection.schema_editor(collect_sql=True) as schema_editor:
                    for operation in migration.operations:
                        operation.database_forwards(
                            label,
                            schema_editor,
                            from_state,
                            to_state
                        )
                    sql_statements = schema_editor.collected_sql

                migration_faked = False
                if sql_statements:
                    for sql in sql_statements:
                        try:
                            db_cursor.execute(sql)
                            self.stdout.write(f"  [OK] Executed: {sql[:80]}{'...' if len(sql) > 80 else ''}")
                            migration_faked = True
                        except Exception as e:
                            self.stdout.write(f"  [IGNORED ERROR] {e} | SQL: {sql[:80]}{'...' if len(sql) > 80 else ''}")
                else:
                    self.stdout.write("  (No SQL generated for this migration)")
                    migration_faked = True  # If no SQL, still mark as fake
                # Mark as fake if at least one SQL ran (or no SQL generated)
                if migration_faked:
                    try:
                        db_cursor.execute(
                            """
                            INSERT INTO django_migrations (app, name, applied)
                            VALUES (%s, %s, %s)
                            """,
                            [label, migration_name, timezone.now()]
                        )
                        self.stdout.write(f"  [FAKE] Marked {label}.{migration_name} as applied in django_migrations.")
                    except Exception as e:
                        self.stdout.write(f"  [IGNORED ERROR] Could not mark as fake: {e}")
        if not found_unmigrated:
            self.stdout.write("All migrations for the specified app(s) are already applied.")
        else:
            self.stdout.write("\nSQL execution complete.")

    def _get_previous_migration(self, loader, app_label, migration_name):
        """Helper to find the previous migration in the dependency graph."""
        migration = loader.disk_migrations[(app_label, migration_name)]
        dependencies = migration.dependencies
        for dep_app, dep_name in dependencies:
            if dep_app == app_label:
                return dep_name
        return None 