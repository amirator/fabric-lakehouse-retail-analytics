# Reference repos studied (2026-10-23)

| Repo | What I borrowed |
|---|---|
| [microsoft/fabric-cicd](https://github.com/microsoft/fabric-cicd) | `FabricWorkspace` + `publish_all_items` deploy script, `parameter.yml` `find_replace` per environment, and `item_type_in_scope` |
| [amxavier/microsoft-fabric-medallion-lakehouse](https://github.com/amxavier/microsoft-fabric-medallion-lakehouse) | Fabric git layout (`*.Notebook/notebook-content.py` with `# META` blocks, `.platform` v2 with logicalId, `*.Lakehouse` items) and a manual-dispatch CD workflow with secrets |
| [kimtth/ms-fabric-auto-maintenance](https://github.com/kimtth/ms-fabric-auto-maintenance) | End-to-end story order (medallion notebooks, then semantic model, then report) and naming of the gold star schema |
| [microsoft/fabric-samples](https://github.com/microsoft/fabric-samples) | Bronze, silver and gold responsibilities (bronze is a raw replayable copy, silver is conformed, gold is dimensional) and Direct Lake on gold Delta tables |
| [microsoft/Analysis-Services](https://github.com/microsoft/Analysis-Services) and [data-goblin/powerbi-macguyver-toolbox](https://github.com/data-goblin/powerbi-macguyver-toolbox) | TMDL/PBIP conventions: `definition/` folder, `ref table` in model.tmdl, `partition … = entity` with `mode: directLake` and `expressionSource`, and stable `lineageTag`s |
