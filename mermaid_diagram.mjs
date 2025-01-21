flowchart TD

inputs@{ shape: lean-right }
internal@{ shape: hex, label: internal/ }
external@{ shape: hex, label: external/ }
plots@{ shape: hex, label: plots/ }
merged@{ shape: docs, label: merged.csv (disease-specific)}

inputs --> internal --> merged
merged --> plots
merged --> |select and filter| external --> merged_release.csv
merged_release.csv --> |select and filter| m@{ shape: processes, label: "...map_data.csv" }
merged_release.csv --> |select and filter| t@{ shape: processes, label: "...timeseries_data.csv" }
m --> |release to| w((website))
t --> |release to| w((website))
merged_release.csv --> |release to| d@{ shape: database, label: "production database\n(in blob storage)" }
d --> |release to| dcg((data.cdc.gov))
d --> |release to| dcp((DCIPHER state explorer))
