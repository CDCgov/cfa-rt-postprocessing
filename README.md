# CFA $R_t$ postprocesing

⚠️ This is a work in progress

## Overview

Code to transform the ouptus of an $R_t$ model run into a standard set plots and
tables for internal evaluation or publication.

### Inputs
This pipeline inputs $R_t$ estimates generated by an upstream model.
The estimates can come from any model, as long as they are formatted following
CFA's standard [schema for $R_t$ estimates]https://github.com/CDCgov/cfa-epinow2-pipeline?tab=readme-ov-file#directories).

```
<model-runs>/
├── job_<job_id>/
│   ├── raw_samples/
│   │   ├── samples_<task_id>.parquet
│   ├── summarized_quantiles/
│   │   ├── summarized_<task_id>.parquet
│   ├── diagnostics/
│   │   ├── diagnostics_<task_id>.parquet
│   ├── tasks/
│   │   ├── task_<task_id>/
│   │   │   ├── model.rds
│   │   │   ├── metadata.json
│   │   │   ├── stdout.log
│   │   │   └── stderr.log
```

### Outputs
The cfa-rt-postprocessing pipeilne generates figures and data tables for
internal evalution and for publication to the [CDC website](), to
[data.cdc.gov](), and to [DCIPHER](). By convention, the pipeline always
generates outputs for publication, even if the input data is from an experiment,
test run, or backfill exercise that is not intended for release. The publication
outputs are not costly to generate and our person-driven release process ensures
that outputs are only released after approval and publication by the team.


```
<root-subdir>/               # E.g. "NSSP_Rt/EpiNow2" or "backfill-experiment"
├── <release-name>/          # E.g. "20241009"
│   ├── internal-review/
|   |   ├── job_<jobid>/
|   |   |   ├── merged.csv
|   |   |   ├── plots/
|   |   |   |   ├── choropleth.png
|   |   |   |   ├── lineinterval.png
|   |   |   |   ├── timeseries/
|   |   |   |   |   ├── <location>.png
|   |   |   |   |   ├── <location>.png
|   |   |   |   ├── ...
|   ├── release/
|   |   ├── merged_release.csv
|   |   ├── <rundate>_<disease1>_map_data.csv
|   |   ├── <rundate>_<disease2>_map_data.csv
|   |   ├── <rundate>_<disease1>_timeseries_data.csv
|   |   ├── <rundate>_<disease2>_timeseries_data.csv
```

- `<root-subdir>/`: The base output directory for processed runs. This could
contain info on the dataset and model (for production runs), or on the
experiment goals (for test runs).
- `<release-name>/`: A subdirectory named to communicate the group of jobs
being processed, e.g. covid and flu jobs for the same run date.
By internal convention this is usually the date the model was run in production,
or for backfill runs, the date of the data snapshot fed into the model.
- `internal_review/` a subdirectory holding outputs for internal review. Outputs
in this subdirectory are job specific. These outputs are generated upstream of
the release outputs, and may contain values that are witheld from release after
review of the models and data, or data that are not released publicly.
- `merged.csv`: A table that summarizes the results of a job and can
be used to generate all downstream plots and publication outputs. Because this
table is designed to support plotting it is in wide format with columns:

  - `release_name`, `task_id`, `geo_value`, `disease`, `model`
  - `reference_date`, `run_date` (date model was run, or as-of date of the data snapshot passed to the model), `final_datapoint` (final date available in the data passed to the model)
  - `rt_median`, `rt_lower`, `rt_upper` (median and 95% crediible intervals for Rt, estimated)
  - `expected_obs_cases_median`, `expected_obs_cases_lower`, `expected_obs_cases_upper` (latent real-time data values, estimated)
  - `pp_nowcast_median`, `pp_nowcast_lower`, `pp_nowcast_upper` (median and 95% posterior prediction interval for nowcasted final data values, estimated)
  - `data_value` (data value reported on `date` in the snapshot available as of `run_date`)
  - `p_growing` (fraction of the Rt credible interval above 1), `category` (Growing, Likely Growing, etc. as defined on the [CDC Rt page](https://www.cdc.gov/cfa-modeling-and-forecasting/rt-estimates/index.html)) - these are NA for all dates except `final_datapoint` and reflect the probabilities and trend categories that would be reported for this run in the map released on the website.

-`plots/` a single choropleth and lineinterval plot for all jurisdictions, and a
location-specific timeseries plot for each jurisdiction
- `release/` a subdirectory holding data files formatted for public release.
- `merged_release.csv` a single long table containing all fields shared
publicly, for all diseases in the same release. Created by binding `merged.csv`
for each disease into a single table, selecting reported fields, and dropping
data excluded after manual review. Contains the fields:  `location` (renamed from `geo_value`), `disease`, `release_date` (??), `rt_median`, `rt_lower`, `rt_upper`, `p_growing`, and `category`
- `..._map_data.csv`, `...timeseries_data.csv` derived from merged_release. We
need to keep producing these until we can make front end changes.

```mermaid
flowchart TD
  inputs[/inputs/];
  internal{{"internal/"}};
  external{{"release/"}};
  plots{{"plots/"}};
  merged["merged.csv (disease-specific)"];

  inputs --> internal --> merged;
  merged --> plots;
  merged --> |select and filter| external --> merged_release.csv;
  merged_release.csv --> |select and filter| m["...map_data.csv"];
  merged_release.csv --> |select and filter| t["...timeseries_data.csv"];
  m --> |release to| w((website));
  t --> |release to| w((website));
  merged_release.csv --> |release to| d[("production database (in blob storage)")];
  d --> |release to| dcg((data.cdc.gov));
  d --> |release to| dcp((DCIPHER state explorer));
```

### Running
To run locally, run `uv run -m src.cli --help` to get the help message explaining how to run it. Note that running this without the `-m src.cli` will result in package paths being read from the wrong relative location, meaning that imports don't work.

## Project Admin

Katie Gostic, uep6@cdc.gov, CDC/IOD/ORR/CFA

## General Disclaimer
This repository was created for use by CDC programs to collaborate on public health related projects in support of the [CDC mission](https://www.cdc.gov/about/organization/mission.htm).  GitHub is not hosted by the CDC, but is a third party website used by CDC and its partners to share information and collaborate on software. CDC use of GitHub does not imply an endorsement of any one particular service, product, or enterprise.

## Public Domain Standard Notice
This repository constitutes a work of the United States Government and is not
subject to domestic copyright protection under 17 USC § 105. This repository is in
the public domain within the United States, and copyright and related rights in
the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).
All contributions to this repository will be released under the CC0 dedication. By
submitting a pull request you are agreeing to comply with this waiver of
copyright interest.

## License Standard Notice
This repository is licensed under ASL v2 or later.

This source code in this repository is free: you can redistribute it and/or modify it under
the terms of the Apache Software License version 2, or (at your option) any
later version.

This source code in this repository is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the Apache Software License for more details.

You should have received a copy of the Apache Software License along with this
program. If not, see http://www.apache.org/licenses/LICENSE-2.0.html

The source code forked from other open source projects will inherit its license.

## Privacy Standard Notice
This repository contains only non-sensitive, publicly available data and
information. All material and community participation is covered by the
[Disclaimer](https://github.com/CDCgov/template/blob/master/DISCLAIMER.md)
and [Code of Conduct](https://github.com/CDCgov/template/blob/master/code-of-conduct.md).
For more information about CDC's privacy policy, please visit [http://www.cdc.gov/other/privacy.html](https://www.cdc.gov/other/privacy.html).

## Contributing Standard Notice
Anyone is encouraged to contribute to the repository by [forking](https://help.github.com/articles/fork-a-repo)
and submitting a pull request. (If you are new to GitHub, you might start with a
[basic tutorial](https://help.github.com/articles/set-up-git).) By contributing
to this project, you grant a world-wide, royalty-free, perpetual, irrevocable,
non-exclusive, transferable license to all users under the terms of the
[Apache Software License v2](http://www.apache.org/licenses/LICENSE-2.0.html) or
later.

All comments, messages, pull requests, and other submissions received through
CDC including this GitHub page may be subject to applicable federal law, including but not limited to the Federal Records Act, and may be archived. Learn more at [http://www.cdc.gov/other/privacy.html](http://www.cdc.gov/other/privacy.html).

## Records Management Standard Notice
This repository is not a source of government records but is a copy to increase
collaboration and collaborative potential. All government records will be
published through the [CDC web site](http://www.cdc.gov).
