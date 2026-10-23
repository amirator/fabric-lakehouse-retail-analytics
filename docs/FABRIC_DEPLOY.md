# Deploying to Microsoft Fabric

> Status: these steps have **not** been executed for this repo, because no Fabric tenant was available. They follow Microsoft's documented flow. Expect small fixes on the first import, such as TMDL property validation or the Lakehouse id binding.

## 1. Get a workspace (free trial)
1. Sign in at <https://app.fabric.microsoft.com> with a work or school Microsoft Entra account. Personal Outlook or Gmail accounts are not accepted.
2. Open the account manager (top-right) and choose **Start trial**. The Fabric trial gives a trial capacity (F64-equivalent) for 60 days.
3. Go to **Workspaces > New workspace**, name it `ws-olist-retail-dev`, and under *Advanced* choose the **Trial** licence mode.

## 2a. Option A: Git integration (no code)
1. Push this repo to GitHub or Azure DevOps.
2. Open **Workspace settings > Git integration**, connect the repo and branch, and set the **Git folder** to `fabric`.
3. Run **Update all**. Fabric creates `lh_olist_retail`, the three notebooks and the `OlistRetail` semantic model.

## 2b. Option B: fabric-cicd (code-first, CI/CD)
```bash
pip install -r requirements-deploy.txt
az login                                   # or set AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET
FABRIC_WORKSPACE_ID=<workspace-guid> FABRIC_ENV=DEV python scripts/deploy_fabric.py
```
`scripts/deploy_fabric.py` publishes `Lakehouse`, `Notebook` and `SemanticModel` items from `fabric/`. `fabric/parameter.yml` swaps the placeholders `__SQL_ENDPOINT__` and `__LAKEHOUSE_ID__` in the model for each environment. For GitHub Actions, add the four secrets and run **deploy-fabric** manually (`.github/workflows/deploy-fabric.yml`). The service principal must be allowed to use the Fabric APIs (tenant setting) and be a workspace Contributor or higher.

## 3. Load data and run the medallion
1. In `lh_olist_retail`, open **Files** and create `raw/olist`. Upload the 8 CSVs from `data/raw` (run `python scripts/download_data.py` locally first). A pipeline Copy activity or a OneLake shortcut works too.
2. Attach `lh_olist_retail` as the default lakehouse for each notebook. Run `nb_01_bronze_ingest`, then `nb_02_silver_conform`, then `nb_03_gold_star_schema`. The printed row counts should match the counts pinned in `tests/test_lakehouse.py`. For example, bronze_orders should be 99,441.
3. Optional: chain the three notebooks in a Data pipeline and schedule it.

## 4. Bind and refresh the semantic model
1. Copy the Lakehouse **SQL analytics endpoint** connection string and the lakehouse id into `fabric/parameter.yml`, then redeploy. You can also edit the `DatabaseQuery` expression in the web model editor.
2. Open `OlistRetail` and use **Refresh** (Direct Lake framing). Check that `[GMV]` = R$ 13,220,248.93 and `[Orders]` = 99,441, matching `kpis.json`.
3. Build a report on the model, or open it in Power BI Desktop through the OneLake data hub.

## 5. Clean up
Delete the workspace when the trial ends to avoid capacity charges on a paid SKU.
