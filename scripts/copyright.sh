#!/bin/bash

readarray -t MISSING_COPYRIGHT < <(
  git ls-files "*.py" | xargs grep --extended-regexp --files-without-match "Copyright \(c\) 20.* Aiven|Aiven license OK"
)

if (( ${#MISSING_COPYRIGHT[@]} != 0 )); then
  echo "Missing default Copyright statement in files:" "${MISSING_COPYRIGHT[@]}"
  false
fi
