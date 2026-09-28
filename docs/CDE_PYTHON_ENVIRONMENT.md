# CDE Python Environment for Great Expectations

Use a **Python Environment** resource in CDE (recommended) so jobs **08** and **09** have `great_expectations` on the Spark driver without runtime `pip install`.

## 1. Create the environment resource

1. Open **CDE** → your virtual cluster (e.g. `cloudera-hol-cde-vc-02`) → **Resources**.
2. Click **Create Resource**.
3. Set:
   - **Resource name:** `holuser01-python-gx` (use your username prefix).
   - **Type:** **Python Environment**.
   - **PyPi Mirror URL:** leave blank unless your admin requires an internal mirror.
4. Click **Create**.

## 2. Add packages

After the resource is created, open it and add dependencies (wording varies slightly by CDE version):

- Upload **`cde/requirements.txt`** from this workshop:

  ```text
  great_expectations==0.18.22
  ```

- Or add the package **`great_expectations==0.18.22`** in the environment UI.

Start the **build / sync** and wait until the environment status is **Ready** (can take several minutes).

## 3. Upload job scripts (separate file resource)

Keep PySpark scripts in a **Files** resource (e.g. `holuser01-insurance`):

- `09_data_quality_customers.py`
- `08_data_quality_great_expectations.py`
- (other `.py` jobs as needed)

Python Environment holds **libraries**; the Files resource holds **your code**.

## 4. Attach the environment to Spark jobs

For each DQ job (`holuser01_09_data_quality_customers`, `holuser01_08_data_quality_great_expectations`):

1. **Jobs** → open the job → **Edit**.
2. **Application file:** select script from your **Files** resource.
3. **Configurations** (or **Advanced**):
   - **Python Environment:** choose `holuser01-python-gx` (the resource from step 1).
4. Save → **Run Now**.

You do **not** need a per-job `requirements.txt` on the Files resource when a Python Environment is selected.

## 5. Verify

Job logs should reach **Step 3** without `ModuleNotFoundError: great_expectations` and show lines like:

```text
  [PASS] customer_id not null
  ...
DATA QUALITY PASSED
```

## Troubleshooting

| Issue | What to do |
|-------|------------|
| Environment stuck building | Check CDE events; confirm PyPI/mirror reachable from the cluster. |
| Job still missing GX | Confirm the job’s **Python Environment** dropdown points to the built resource, not “Default”. |
| Build fails on GX 0.18 | Try `great_expectations==0.18.21` or ask admin for an approved version. |
| Internal PyPI only | Set **PyPi Mirror URL** when creating the environment resource. |

## Fallback in scripts

`08` and `09` still call `ensure_great_expectations()` (driver `pip install`) if GX is missing. Prefer the **Python Environment** resource; use the fallback only when pip/network is allowed and you cannot build an environment.
