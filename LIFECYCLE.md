# Repository lifecycle

**State:** `maintenance`  
**Decision date:** 2026-08-03  
**Review cadence:** annual  
**Next portfolio review:** August 2027

## Why this state

This repository is retained as a reproducible sampling utility for Argentina's 2010 census microdata. It remains useful when a current research workflow needs synthetic household samples, but it is not under continuous feature development.

## Maintenance policy

- Preserve the command-line sampling capability and reproducibility assumptions.
- Verify the documented CLI against an authorized local copy of the census tables during the annual review or when a current consumer appears.
- Correct observed breakage or misleading documentation.
- Do not add new formats, data vintages, packaging layers, or abstractions without a named consumer.
- Do not commit census microdata or generated samples merely to simplify verification.

## Verification boundary

This lifecycle declaration does not certify that the current dependencies, example commands, online notebook/site links, or output formats work on 2026-08-03.

A meaningful verification should record:

1. census source and local path;
2. command and sampling parameters;
3. deterministic seed behavior;
4. row counts and output paths;
5. data-license and privacy handling;
6. any divergence between the README and implemented CLI.

## Data responsibility

The repository contains sampling code, not authority over the underlying census data. Users remain responsible for obtaining and handling source microdata lawfully and securely.
