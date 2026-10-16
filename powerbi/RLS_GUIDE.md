# Power BI Row-Level Security (RLS) Guide

This guide describes how to design and apply Row-Level Security (RLS) in Power BI Desktop to restrict data access for given users based on dimensional filters.

---

## 1. What is Row-Level Security?

Row-Level Security (RLS) restricts data access at the row level based on the identity of the user viewing the report. When RLS is applied, visuals and DAX calculations only compute over rows the user has permission to see.

> [!IMPORTANT]
> **Configuration Boundary**: RLS rules are authored in Power BI Desktop and mapped to user security groups inside the Power BI Service. The pipeline provides architectural recommendations and filter patterns, but RLS is not active until explicitly enabled in Power BI.

---

## 2. Static vs. Dynamic RLS

### A. Static Row-Level Security
* **Concept**: Creating a fixed role for each distinct dimension value (e.g., Department A, Department B, Region 1).
* **Setup in Power BI Desktop**:
  1. On the **Modeling** ribbon, select **Manage Roles**.
  2. Click **Create** and name the role (e.g., `Category_A_Viewer`).
  3. Under **Tables**, select the target table and enter a DAX filter expression:
     ```dax
     [category_column] = "Category_A"
     ```
  4. Click **Save**.
* **Limitation**: Requires creating a separate role for every distinct value, making it unscalable for high-cardinality dimensions.

### B. Dynamic Row-Level Security (Recommended)
* **Concept**: A single generic role that looks up the logged-in user's email address using `USERPRINCIPALNAME()` against a security mapping table.
* **Architecture**:
  ```text
  User Access Table              Analytical Fact Table
  [Email] | [Allowed_Dimension]  ---> [Dimension_Column] | [Metric]
  ```
* **Setup in Power BI Desktop**:
  1. In the **Modeling** tab, select **Manage Roles**.
  2. Create a generic role named `DynamicUserSecurity`.
  3. Under the security mapping table, apply the DAX filter:
     ```dax
     [UserEmail] = USERPRINCIPALNAME()
     ```
  4. If the security table has a 1-to-many relationship with cross-filtering applied to the analytical table, security filters automatically propagate downstream to all related facts.

---

## 3. Testing and Validating RLS in Power BI Desktop

1. On the **Modeling** ribbon, click **View as**.
2. To test static roles: Check the role name (e.g. `Category_A_Viewer`) and click **OK**. Verify that visual tables and KPIs only show rows matching the rule.
3. To test dynamic roles: Check **Other user**, enter an email address (e.g. `analyst@organization.com`), check `DynamicUserSecurity`, and click **OK**.
4. Click **Stop Viewing** on the top yellow banner to return to admin view.

---

## 4. Power BI Service Deployment

1. Publish the `.pbix` report to a Power BI Service Workspace.
2. In the Service, navigate to the dataset settings -> **Security**.
3. Under the configured roles, assign users, Microsoft 365 security groups, or distribution lists.
4. Test the deployed security using the **Test as role** ellipsis option in the Service.
