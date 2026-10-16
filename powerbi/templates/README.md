# Power BI Templates & Binary File Guidance

This directory explains the architecture and operational guidance regarding Power BI project files (`.pbix` and `.pbit`).

---

## 1. Why No Fabricated `.pbix` / `.pbit` Binary Files?

Microsoft Power BI `.pbix` (Power BI Report) and `.pbit` (Power BI Template) files are proprietary, compressed compound zip archives containing:
1. Microsoft VertiPaq internal database snapshots.
2. Compiled Mashup (Power Query M) binary packages.
3. Proprietary WPF report layout and visual state schemas.

Generating arbitrary binary `.pbix` files outside of Microsoft Power BI Desktop or the official Power BI Developer API tools creates invalid or corrupted archives that fail to open in Power BI Desktop.

In accordance with Data Engineering integrity standards, this repository **does not fabricate fake `.pbix` binary files**.

---

## 2. Fast-Start Assembly (Under 2 Minutes)

You can construct a production-ready Power BI dashboard in Power BI Desktop in just a few clicks using the generated artifacts:

1. **Open Power BI Desktop**.
2. **Connect**: Follow [CONNECTION_GUIDE.md](../CONNECTION_GUIDE.md) to connect to your PostgreSQL database.
3. **Inspect Models**: Review [semantic_model.json](../semantic_model.json) to see the automatically detected tables, column roles, and relationships.
4. **Add Measures**: Open [dax/generic_measures.dax](../dax/generic_measures.dax) and copy the pre-built DAX measures into your model.
5. **Assemble Visuals**: Follow [dashboard_spec.json](../dashboard_spec.json) and [VISUALIZATION_GUIDE.md](../VISUALIZATION_GUIDE.md) to place recommended visual types on the canvas.
6. **Save**: Click **File -> Save As** to save your official `.pbix` file locally or publish to Power BI Service.
