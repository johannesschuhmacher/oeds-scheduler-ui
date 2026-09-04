from datetime import datetime, timedelta

from oeds_scheduler_ui.interfaces import (
    CrawlerRegistry,
    CrawlerSpec,
    load_crawler_target,
    merge_crawler_registries,
    normalize_crawler_config,
    registry_from_spec_strings,
    run_crawler_instance,
)
from oeds_scheduler_ui.distribution import registries_from_inventory
from oeds_scheduler_ui.discovery import discover_crawler_specs
from oeds_scheduler_ui.factory import (
    CONSTRUCTOR_CRAWLER_NAME_CONFIG,
    CONSTRUCTOR_SCHEMA_NAME_CONFIG,
    CONSTRUCTOR_SCHEMA_NAME_ONLY,
    CONSTRUCTOR_UNKNOWN,
    CrawlerFactory,
)
from oeds_scheduler_ui.planner import (
    build_scheduler_job_plans,
    expand_crawler_job_configs,
    merge_job_config,
)
from oeds_scheduler_ui.runtime import (
    CrawlerJobQueue,
    CrawlerJobRunner,
    PostRunCommandResult,
    execute_post_command,
    lock_keys_from_plan,
    run_ready_jobs,
    scheduled_job_from_plan,
)
from oeds_scheduler_ui.service import SchedulerService
from oeds_scheduler_ui.daemon import SchedulerDaemon


def test_normalize_crawler_config_accepts_kit_database_uri():
    config = normalize_crawler_config(
        {
            "schema_name": "smard",
            "database_uri": "postgresql://user:pass@localhost:5432/opendata",
            "schedule": "0 4 * * *",
            "enable": True,
            "region": "DE",
        }
    )

    assert config.schema_name == "smard"
    assert config.db_uri == "postgresql://user:pass@localhost:5432/opendata"
    assert config.options == {"region": "DE"}


def test_run_crawler_instance_prefers_run():
    class Crawler:
        def run(self):
            return "run"

        def crawl_temporal(self):
            return "crawl_temporal"

    assert run_crawler_instance(Crawler()) == "run"


def test_run_crawler_instance_supports_temporal_legacy_crawler():
    class Crawler:
        def crawl_temporal(self):
            return "crawl_temporal"

    assert run_crawler_instance(Crawler()) == "crawl_temporal"


def test_merge_crawler_registries_keeps_first_registry_priority():
    class CoreSmard:
        pass

    class KitSmard:
        pass

    class EntsoeFms:
        pass

    merged = merge_crawler_registries(
        [
            CrawlerRegistry("oeds-crawler-pack", {"smard": KitSmard, "entsoe_fms": EntsoeFms}),
            CrawlerRegistry("oeds-core", {"smard": CoreSmard}),
        ]
    )

    assert merged["smard"] is KitSmard
    assert merged["entsoe_fms"] is EntsoeFms


def test_registry_from_spec_strings_creates_lazy_targets(tmp_path):
    registry = registry_from_spec_strings(
        "oeds-crawler-pack",
        {"smard": "crawler.smard:SmardCrawler"},
        source_path=tmp_path,
    )

    target = registry.crawlers["smard"]

    assert isinstance(target, CrawlerSpec)
    assert target.source_name == "oeds-crawler-pack"
    assert target.module == "crawler.smard"
    assert target.attribute == "SmardCrawler"
    assert target.source_path == tmp_path.resolve()


def test_load_crawler_target_imports_from_source_path(tmp_path):
    package = tmp_path / "example_crawlers"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "demo.py").write_text(
        "class DemoCrawler:\n    pass\n",
        encoding="utf-8",
    )
    spec = CrawlerSpec.parse(
        "test-source",
        "example_crawlers.demo:DemoCrawler",
        source_path=tmp_path,
    )

    crawler_class = load_crawler_target(spec)

    assert crawler_class.__name__ == "DemoCrawler"


def test_pilot_registry_keeps_kit_priority_and_upstream_only_crawlers(tmp_path):
    kit_registry = registry_from_spec_strings(
        "oeds-crawler-pack",
        {
            "smard": "crawler.smard:SmardCrawler",
            "eurostat_crawler": "crawler.eurostat_crawler:EurostatCrawler",
        },
        source_path=tmp_path / "kit",
    )
    core_registry = registry_from_spec_strings(
        "oeds-core",
        {
            "smard": "oeds.crawler.smard:SmardCrawler",
            "chargepoint": "oeds.crawler.chargepoint:ChargepointDownloader",
        },
        source_path=tmp_path / "core",
    )

    merged = merge_crawler_registries([kit_registry, core_registry])

    assert set(merged) == {"smard", "eurostat_crawler", "chargepoint"}
    assert merged["smard"].source_name == "oeds-crawler-pack"
    assert merged["eurostat_crawler"].source_name == "oeds-crawler-pack"
    assert merged["chargepoint"].source_name == "oeds-core"


def test_registries_from_inventory_preserves_priority(tmp_path):
    inventory = {
        "registry_priority": ["oeds-crawler-pack", "oeds-core"],
        "pilot": {
            "smard": {
                "preferred_source": "oeds-crawler-pack",
                "source_path": ".",
                "module": "crawler.smard",
                "attribute": "SmardCrawler",
            },
            "chargepoint": {
                "preferred_source": "oeds-core",
                "source_path": ".",
                "module": "oeds.crawler.chargepoint",
                "attribute": "ChargepointDownloader",
            },
        },
    }

    registries = registries_from_inventory(inventory, workspace_root=tmp_path)
    merged = merge_crawler_registries(registries)

    assert [registry.source_name for registry in registries] == [
        "oeds-crawler-pack",
        "oeds-core",
    ]
    assert merged["smard"].source_name == "oeds-crawler-pack"
    assert merged["chargepoint"].source_name == "oeds-core"


def test_crawler_factory_audits_constructor_styles(tmp_path):
    kit_package = tmp_path / "crawler"
    kit_package.mkdir()
    (kit_package / "__init__.py").write_text("", encoding="utf-8")
    (kit_package / "smard.py").write_text(
        "class SmardCrawler:\n"
        "    def __init__(self, crawler_name, config):\n"
        "        pass\n"
        "    def run(self):\n"
        "        pass\n",
        encoding="utf-8",
    )
    core_package = tmp_path / "oeds" / "crawler"
    core_package.mkdir(parents=True)
    (tmp_path / "oeds" / "__init__.py").write_text("", encoding="utf-8")
    (core_package / "__init__.py").write_text("", encoding="utf-8")
    (core_package / "chargepoint.py").write_text(
        "class ChargepointDownloader:\n"
        "    def __init__(self, schema_name, config):\n"
        "        pass\n"
        "    def crawl_structural(self):\n"
        "        pass\n",
        encoding="utf-8",
    )
    registries = [
        registry_from_spec_strings(
            "oeds-crawler-pack",
            {"smard": "crawler.smard:SmardCrawler"},
            source_path=tmp_path,
        ),
        registry_from_spec_strings(
            "oeds-core",
            {"chargepoint": "oeds.crawler.chargepoint:ChargepointDownloader"},
            source_path=tmp_path,
        ),
    ]
    factory = CrawlerFactory(merge_crawler_registries(registries))

    smard_audit = factory.audit("smard")
    chargepoint_audit = factory.audit("chargepoint")

    assert smard_audit.constructor_style == CONSTRUCTOR_CRAWLER_NAME_CONFIG
    assert smard_audit.run_methods == ("run",)
    assert chargepoint_audit.constructor_style == CONSTRUCTOR_SCHEMA_NAME_CONFIG
    assert chargepoint_audit.run_methods == ("crawl_structural",)


def test_crawler_factory_supports_schema_name_only_legacy_constructor(tmp_path):
    core_package = tmp_path / "oeds" / "crawler"
    core_package.mkdir(parents=True)
    (tmp_path / "oeds" / "__init__.py").write_text("", encoding="utf-8")
    (core_package / "__init__.py").write_text("", encoding="utf-8")
    (core_package / "eex.py").write_text(
        "class EEXCrawler:\n"
        "    def __init__(self, schema_name):\n"
        "        pass\n"
        "    def crawl_structural(self):\n"
        "        pass\n",
        encoding="utf-8",
    )
    registry = registry_from_spec_strings(
        "oeds-core",
        {"eex": "oeds.crawler.eex:EEXCrawler"},
        source_path=tmp_path,
    )
    factory = CrawlerFactory(merge_crawler_registries([registry]))
    config = normalize_crawler_config(
        {
            "schema_name": "eex_prices",
            "database_uri": "postgresql://user:pass@localhost:5432/opendata",
        }
    )

    audit = factory.audit("eex")
    plan = factory.constructor_plan("eex", config)

    assert audit.constructor_style == CONSTRUCTOR_SCHEMA_NAME_ONLY
    assert audit.has_supported_constructor is True
    assert plan.args == ("eex_prices",)


def test_crawler_factory_rejects_additional_required_constructor_parameter(tmp_path):
    core_package = tmp_path / "oeds" / "crawler"
    core_package.mkdir(parents=True)
    (tmp_path / "oeds" / "__init__.py").write_text("", encoding="utf-8")
    (core_package / "__init__.py").write_text("", encoding="utf-8")
    (core_package / "dwd.py").write_text(
        "class DWDCrawler:\n"
        "    def __init__(self, schema_name, config, nuts_matrix):\n"
        "        pass\n"
        "    def crawl_temporal(self):\n"
        "        pass\n",
        encoding="utf-8",
    )
    registry = registry_from_spec_strings(
        "oeds-core",
        {"dwd": "oeds.crawler.dwd:DWDCrawler"},
        source_path=tmp_path,
    )
    factory = CrawlerFactory(merge_crawler_registries([registry]))

    audit = factory.audit("dwd")

    assert audit.init_parameters == ("schema_name", "config", "nuts_matrix")
    assert audit.required_init_parameters == (
        "schema_name",
        "config",
        "nuts_matrix",
    )
    assert audit.constructor_style == CONSTRUCTOR_UNKNOWN
    assert audit.has_supported_constructor is False


def test_crawler_factory_allows_optional_constructor_extension(tmp_path):
    crawler_package = tmp_path / "crawler"
    crawler_package.mkdir()
    (crawler_package / "__init__.py").write_text("", encoding="utf-8")
    (crawler_package / "sample.py").write_text(
        "class SampleCrawler:\n"
        "    def __init__(self, crawler_name, config, batch_size=100):\n"
        "        pass\n"
        "    def run(self):\n"
        "        pass\n",
        encoding="utf-8",
    )
    registry = registry_from_spec_strings(
        "oeds-crawler-pack",
        {"sample": "crawler.sample:SampleCrawler"},
        source_path=tmp_path,
    )
    factory = CrawlerFactory(merge_crawler_registries([registry]))

    audit = factory.audit("sample")

    assert audit.required_init_parameters == ("crawler_name", "config")
    assert audit.constructor_style == CONSTRUCTOR_CRAWLER_NAME_CONFIG


def test_discover_crawler_specs_finds_class_based_crawlers(tmp_path):
    crawler_dir = tmp_path / "crawler"
    crawler_dir.mkdir()
    (crawler_dir / "__init__.py").write_text("", encoding="utf-8")
    (crawler_dir / "smard.py").write_text(
        "class SmardCrawler(BaseCrawler):\n"
        "    def __init__(self, crawler_name, config):\n"
        "        pass\n"
        "    def run(self):\n"
        "        pass\n",
        encoding="utf-8",
    )
    (crawler_dir / "helper.py").write_text("def main():\n    pass\n", encoding="utf-8")

    discovery = discover_crawler_specs(
        source_name="oeds-crawler-pack",
        source_path=tmp_path,
        crawler_package_path="crawler",
        module_prefix="crawler",
    )

    assert list(discovery.crawlers) == ["smard"]
    assert discovery.crawlers["smard"].attribute == "SmardCrawler"
    assert [issue.module_name for issue in discovery.issues] == ["crawler.helper"]


def test_planner_expands_named_jobs_with_inherited_config():
    config = {
        "enable": True,
        "schema_name": "entsoe_fms",
        "database_uri": "postgresql://user:pass@localhost:5432/opendata",
        "post_run_scripts": ["scripts/gapfill_timeseries.py"],
        "jobs": {
            "latest_hourly": {
                "enable": True,
                "schedule": "0 * * * *",
                "target_data_items": ["ActualTotalLoad_6.1.A_r3"],
            },
            "revision_sweep_daily": {
                "enable": True,
                "schedule": "30 2 * * *",
                "run_post_scripts": False,
            },
        },
    }

    jobs = expand_crawler_job_configs("entsoe_fms", config)

    assert [job.job_id for job in jobs] == [
        "entsoe_fms:latest_hourly",
        "entsoe_fms:revision_sweep_daily",
    ]
    assert jobs[0].config["post_run_scripts"] == ["scripts/gapfill_timeseries.py"]
    assert jobs[0].config["target_data_items"] == ["ActualTotalLoad_6.1.A_r3"]
    assert jobs[1].run_post_scripts is False


def test_planner_builds_dry_job_plans_without_importing_crawlers(tmp_path):
    crawler_dir = tmp_path / "crawler"
    crawler_dir.mkdir()
    (crawler_dir / "__init__.py").write_text("", encoding="utf-8")
    (crawler_dir / "smard.py").write_text(
        "class SmardCrawler:\n"
        "    def __init__(self, crawler_name, config):\n"
        "        pass\n"
        "    def run(self):\n"
        "        return None\n",
        encoding="utf-8",
    )
    registry = registry_from_spec_strings(
        "oeds-crawler-pack",
        {"smard": "crawler.smard:SmardCrawler"},
        source_path=tmp_path,
    )
    factory = CrawlerFactory(merge_crawler_registries([registry]))

    result = build_scheduler_job_plans(
        {
            "default": {
                "enable": False,
                "schedule": "0 4 * * *",
                "database_uri": "postgresql://user:pass@localhost:5432/opendata",
            },
            "smard": {
                "enable": True,
                "schema_name": "smard",
                "post_run_scripts": ["scripts/gapfill_smard.py"],
            },
            "missing": {
                "enable": True,
                "schema_name": "missing",
            },
        },
        factory,
    )

    assert not result.errors
    assert len(result.plans) == 1
    assert result.plans[0].job_id == "smard:default"
    assert result.plans[0].source_name == "oeds-crawler-pack"
    assert result.plans[0].constructor_plan.args[0] == "smard"
    assert result.plans[0].post_run_scripts == ("scripts/gapfill_smard.py",)
    assert result.skipped[0].crawler_name == "missing"


def test_planner_reports_crawlers_without_supported_run_method(tmp_path):
    core_package = tmp_path / "oeds" / "crawler"
    core_package.mkdir(parents=True)
    (tmp_path / "oeds" / "__init__.py").write_text("", encoding="utf-8")
    (core_package / "__init__.py").write_text("", encoding="utf-8")
    (core_package / "eex.py").write_text(
        "class EEXCrawler:\n"
        "    def __init__(self, schema_name):\n"
        "        pass\n",
        encoding="utf-8",
    )
    registry = registry_from_spec_strings(
        "oeds-core",
        {"eex": "oeds.crawler.eex:EEXCrawler"},
        source_path=tmp_path,
    )
    factory = CrawlerFactory(merge_crawler_registries([registry]))

    result = build_scheduler_job_plans(
        {
            "default": {
                "enable": True,
                "database_uri": "postgresql://user:pass@localhost:5432/opendata",
            },
            "eex": {
                "enable": True,
                "schema_name": "eex_prices",
            },
        },
        factory,
    )

    assert not result.plans
    assert result.errors[0].reason == "crawler has no supported run method"


def test_merge_job_config_recursively_overrides_nested_values():
    merged = merge_job_config(
        {"email": {"subject": "old", "toaddrs": []}, "enable": False},
        {"email": {"subject": "new"}, "enable": True},
    )

    assert merged == {
        "email": {"subject": "new", "toaddrs": []},
        "enable": True,
    }


def test_runtime_executes_planned_job_and_post_run_hook():
    class RuntimeCrawler:
        constructed = []
        runs = []

        def __init__(self, crawler_name, config):
            self.crawler_name = crawler_name
            self.config = config
            RuntimeCrawler.constructed.append((crawler_name, dict(config)))

        def run(self):
            RuntimeCrawler.runs.append(self.crawler_name)
            return "done"

    registry = CrawlerRegistry("runtime-test", {"runtime_crawler": RuntimeCrawler})
    factory = CrawlerFactory(merge_crawler_registries([registry]))
    plan_result = build_scheduler_job_plans(
        {
            "default": {
                "enable": True,
                "schedule": "0 4 * * *",
                "database_uri": "postgresql://user:pass@localhost:5432/opendata",
            },
            "runtime_crawler": {
                "schema_name": "runtime_schema",
                "post_run_scripts": ["scripts/after_runtime.py"],
            },
        },
        factory,
    )
    post_run_commands = []

    def fake_post_run(command, plan):
        post_run_commands.append((plan.job_id, command))
        return PostRunCommandResult(command, 0, True)

    result = CrawlerJobRunner(factory, fake_post_run).run(plan_result.plans[0])

    assert result.success is True
    assert result.crawler_result == "done"
    assert RuntimeCrawler.constructed[0][0] == "runtime_crawler"
    assert RuntimeCrawler.constructed[0][1]["schema_name"] == "runtime_schema"
    assert RuntimeCrawler.runs == ["runtime_crawler"]
    assert post_run_commands == [
        ("runtime_crawler:default", "scripts/after_runtime.py")
    ]


def test_execute_post_command_adds_workspace_to_pythonpath_for_legacy_scripts(
    tmp_path,
    monkeypatch,
):
    package = tmp_path / "workspace_pkg"
    package.mkdir()
    (package / "__init__.py").write_text('VALUE = "ok"\n', encoding="utf-8")
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "check_import.py").write_text(
        "from workspace_pkg import VALUE\n"
        "raise SystemExit(0 if VALUE == 'ok' else 1)\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    result = execute_post_command("scripts/check_import.py", plan=None)

    assert result.success is True


def test_runtime_queue_blocks_conflicting_lock_keys():
    class RuntimeCrawler:
        def __init__(self, crawler_name, config):
            self.crawler_name = crawler_name

        def run(self):
            return self.crawler_name

    registry = CrawlerRegistry("runtime-test", {"runtime_crawler": RuntimeCrawler})
    factory = CrawlerFactory(merge_crawler_registries([registry]))
    plan_result = build_scheduler_job_plans(
        {
            "default": {
                "enable": True,
                "database_uri": "postgresql://user:pass@localhost:5432/opendata",
            },
            "runtime_crawler": {
                "schema_name": "runtime_schema",
                "jobs": {
                    "a": {"enable": True, "lock_keys": ["shared"]},
                    "b": {"enable": True, "lock_keys": ["shared"]},
                },
            },
        },
        factory,
    )
    jobs = [scheduled_job_from_plan(plan) for plan in plan_result.plans]
    queue = CrawlerJobQueue()

    assert queue.enqueue(jobs[0]) is True
    assert queue.enqueue(jobs[0]) is False
    assert queue.enqueue(jobs[1]) is True
    first_ready = queue.pop_ready_jobs()
    assert len(first_ready) == 1
    assert first_ready[0].job.job_id == "runtime_crawler:a"
    assert queue.pop_ready_jobs() == ()
    queue.mark_finished(jobs[0])
    second_ready = queue.pop_ready_jobs()
    assert len(second_ready) == 1
    assert second_ready[0].job.job_id == "runtime_crawler:b"


def test_runtime_lock_keys_for_entsoe_fms_target_data_items():
    class EntsoeFMSCrawler:
        def __init__(self, schema_name, config):
            self.schema_name = schema_name

        def run(self):
            return self.schema_name

    registry = CrawlerRegistry("runtime-test", {"entsoe_fms": EntsoeFMSCrawler})
    factory = CrawlerFactory(merge_crawler_registries([registry]))
    plan_result = build_scheduler_job_plans(
        {
            "default": {
                "enable": True,
                "database_uri": "postgresql://user:pass@localhost:5432/opendata",
            },
            "entsoe_fms": {
                "schema_name": "entsoe_fms",
                "target_data_items": ["ActualTotalLoad_6.1.A_r3"],
            },
        },
        factory,
    )

    assert lock_keys_from_plan(plan_result.plans[0]) == frozenset(
        {"entsoe_fms:ActualTotalLoad_6.1.A_r3"}
    )


def test_run_ready_jobs_marks_jobs_finished_after_execution():
    class RuntimeCrawler:
        runs = []

        def __init__(self, crawler_name, config):
            self.crawler_name = crawler_name

        def run(self):
            RuntimeCrawler.runs.append(self.crawler_name)

    registry = CrawlerRegistry("runtime-test", {"runtime_crawler": RuntimeCrawler})
    factory = CrawlerFactory(merge_crawler_registries([registry]))
    plan = build_scheduler_job_plans(
        {
            "default": {
                "enable": True,
                "database_uri": "postgresql://user:pass@localhost:5432/opendata",
            },
            "runtime_crawler": {"schema_name": "runtime_schema"},
        },
        factory,
    ).plans[0]
    queue = CrawlerJobQueue()
    queue.enqueue(scheduled_job_from_plan(plan))

    results = run_ready_jobs(queue, CrawlerJobRunner(factory))

    assert len(results) == 1
    assert results[0].success is True
    assert RuntimeCrawler.runs == ["runtime_crawler"]


def test_scheduler_service_enqueues_due_jobs_with_injected_schedule():
    class RuntimeCrawler:
        runs = []

        def __init__(self, crawler_name, config):
            self.crawler_name = crawler_name

        def run(self):
            RuntimeCrawler.runs.append(self.crawler_name)
            return self.crawler_name

    class EverySecondSchedule:
        def next_after(self, ref_time):
            return ref_time + timedelta(seconds=1)

    registry = CrawlerRegistry("runtime-test", {"runtime_crawler": RuntimeCrawler})
    factory = CrawlerFactory(merge_crawler_registries([registry]))
    plan = build_scheduler_job_plans(
        {
            "default": {
                "enable": True,
                "schedule": "1",
                "database_uri": "postgresql://user:pass@localhost:5432/opendata",
            },
            "runtime_crawler": {"schema_name": "runtime_schema"},
        },
        factory,
    ).plans[0]
    reference_time = datetime(2026, 1, 1, 0, 0, 0)
    service = SchedulerService(
        [plan],
        CrawlerJobRunner(factory),
        schedule_factory=lambda expression: EverySecondSchedule(),
        reference_time=reference_time - timedelta(seconds=1),
    )

    assert not service.issues
    assert service.next_run_time == reference_time
    first_results = service.tick(reference_time)
    assert len(first_results) == 1
    assert first_results[0].success is True
    assert RuntimeCrawler.runs == ["runtime_crawler"]
    assert service.next_run_time == reference_time + timedelta(seconds=1)
    assert service.tick(reference_time) == ()


def test_scheduler_service_reports_missing_schedule():
    class RuntimeCrawler:
        def __init__(self, crawler_name, config):
            pass

        def run(self):
            pass

    registry = CrawlerRegistry("runtime-test", {"runtime_crawler": RuntimeCrawler})
    factory = CrawlerFactory(merge_crawler_registries([registry]))
    plan = build_scheduler_job_plans(
        {
            "default": {
                "enable": True,
                "database_uri": "postgresql://user:pass@localhost:5432/opendata",
            },
            "runtime_crawler": {
                "schema_name": "runtime_schema",
                "schedule": None,
            },
        },
        factory,
    ).plans[0]

    service = SchedulerService(
        [plan],
        CrawlerJobRunner(factory),
        schedule_factory=lambda expression: None,
    )

    assert service.jobs == ()
    assert service.issues[0].job_id == "runtime_crawler:default"
    assert service.issues[0].reason == "job has no schedule"


def test_scheduler_daemon_runs_one_tick_and_reports_next_wait():
    class RuntimeCrawler:
        def __init__(self, crawler_name, config):
            self.crawler_name = crawler_name

        def run(self):
            return self.crawler_name

    class EverySecondSchedule:
        def next_after(self, ref_time):
            return ref_time + timedelta(seconds=1)

    registry = CrawlerRegistry("runtime-test", {"runtime_crawler": RuntimeCrawler})
    factory = CrawlerFactory(merge_crawler_registries([registry]))
    plan = build_scheduler_job_plans(
        {
            "default": {
                "enable": True,
                "schedule": "1",
                "database_uri": "postgresql://user:pass@localhost:5432/opendata",
            },
            "runtime_crawler": {"schema_name": "runtime_schema"},
        },
        factory,
    ).plans[0]
    reference_time = datetime(2026, 1, 1, 0, 0, 0)
    service = SchedulerService(
        [plan],
        CrawlerJobRunner(factory),
        schedule_factory=lambda expression: EverySecondSchedule(),
        reference_time=reference_time - timedelta(seconds=1),
    )

    class App:
        def __init__(self, service):
            self.service = service

        def tick(self, now):
            return self.service.tick(now)

    daemon = SchedulerDaemon(
        App(service),
        poll_seconds=30.0,
        now_func=lambda: reference_time,
    )

    assert daemon.seconds_until_next_tick(reference_time) == 0.0
    results = daemon.run_once()
    assert len(results) == 1
    assert results[0].crawler_result == "runtime_crawler"
    assert daemon.seconds_until_next_tick(reference_time) == 1.0
