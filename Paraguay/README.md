# Paraguay BOOST expenditures

Paraguay's pipeline starts from the two expenditure files the Ministry of Economy and Finance (MEF) sends and ends
in the BOOST gold table.

| file | does | output |
|---|---|---|
| `PRY_extract_raw_microdata_excel_to_csv.py` | reads the ministry files, renames the columns to the BOOST names, adds the transfer, foreign, road and hospital flags and the municipal department | `raw_microdata_csv/Paraguay/cen.csv`, `Municipalidades.csv` |
| `PRY_transform_load_dlt.py` | tags `func`, `func_sub`, `econ`, `econ_sub`, `admin0-2`, `geo1` | `pry_boost_bronze_cen`, `pry_boost_bronze_municipal`, `pry_boost_silver`, `pry_boost_gold` |

`PRY_extract_microdata.py` is the older notebook that read the BOOST workbook instead of the ministry files.

## Inputs

### Ministry files: `Data from authorities/Paraguay/`

Two Excel files, each with all years in one sheet named `Database` and one row per budget line. The extract expects
exactly one file per pattern.

**Central and decentralised administration**: `Gastos Administraci*n Central y Descentralizadas*.xlsx`
(currently `... 2003-2024.xlsx`). Amounts in guaraníes. Labels are `code - name`, e.g. `833 - TRANSFERENCIAS A
MUNICIPALIDADES`.

| ministry header | BOOST name | | ministry header | BOOST name |
|---|---|---|---|---|
| Año | YEAR | | Sub-grupo económico | ECON2 |
| Nivel | ADMIN1 | | Económico | ECON3 |
| Entidad | ADMIN2 | | Grupo Objeto del Gasto | ECON4 |
| Unidad Responsable | ADMIN3 | | Sub-grupo Objeto del Gasto | ECON5 |
| Tipo Presupuesto | BUDGET_TYPE | | Objeto del Gasto | ECON6 |
| Programa | PROGRAM1 | | Fuente de Financiamiento | FIN_SOURCE1 |
| Sub-programa | PROGRAM2 | | Origen de Financiamiento | FIN_SOURCE2 |
| Proyecto | PROJECT1 | | Departamento | GEO1 |
| Finalidad | FUNCTION1 | | Transferencia Consolidable | TRANSFER |
| Función | FUNCTION2 | | Presupuesto Inicial | APPROVED |
| Sub-función | FUNCTION3 | | Presupuesto Vigente | MODIFIED |
| Grupo Económico | ECON1 | | Ejecutado | COMMITTED |
| | | | Pagado | PAID |

**Municipalities**: `Gastos*Municipios*.xlsx` (currently `Gastos-anuales-Municipios-2006-2024.xlsx`). Amounts in
millions of guaraníes; the extract converts them to guaraníes.

| ministry header | BOOST name |
|---|---|
| Año | YEAR |
| Nivel | ADMIN1 |
| Entidad | ADMIN2 (e.g. `30.001 - ASUNCION`) |
| Grupo Objeto del Gasto | ECON4 |
| Sub-Grupo Objeto del Gasto | ECON5 |
| Presupuesto Vigente (millones de guaraníes) | MODIFIED |
| Ejecutado (millones de guaraníes) | COMMITTED |
| Pagado (millones de guaraníes) | PAID |

The municipal file has no initial budget, no `Objeto del Gasto` (ECON6), no functional classification and no
location. Other columns in either file are ignored. Years before 2006 are dropped.

### Mapping files: `Documents/input/Auxiliary/Paraguay/`

| file | columns | used for |
|---|---|---|
| `PRY_project_mapping.csv` | `project1` (the PROJECT1 label), `road` (`y` or `n`), `reason` | the Road flag: the reviewed decision for every project name the road key words match, plus names they miss |
| `PRY_admin_mapping.csv` | `admin2_code` (e.g. `30.001`), `municipality`, `department` (e.g. `00 - ASUNCION`) | the department of each municipality, which becomes `admin1` and `geo1` of the municipal rows |

## Outputs

`cen.csv` holds the central columns above plus `TRANS` (ECON6 833/836/893/896), `Foreign` (FIN_SOURCE2 codes of
foreign sources), `Road` and `hospital`. `Municipalidades.csv` holds `YEAR, ADMIN1, ADMIN2, ECON4, ECON5, ECON6,
geo, approved, MODIFIED, COMMITTED, PAID`, with `ECON6` and `approved` empty. Both are UTF-8 with accents stripped
from the labels. In the gold table, municipal `approved` is Presupuesto Vigente and `executed` is Ejecutado for both
tiers.

## Updating to a new year

1. **Get the files.** Ask the MEF for the two files with the new year added. Put them in
   `Data from authorities/Paraguay/` and move the previous versions out, so each pattern matches one file.
2. **Check the layout.** The extract reads one file per tier with all years in the `Database` sheet. If the new
   delivery is shaped differently (one file per year, other sheets, renamed headers), adapt `read_database` first
   (see its TODO). A missing or renamed column stops the run with its name.
3. **Run `PRY_extract_raw_microdata_excel_to_csv.py`.** It stops, listing the names, when:
   - a project name matches the road key words but is not in `PRY_project_mapping.csv`. Add each one with `road`
     `y` or `n` and a reason;
   - a municipality code is not in `PRY_admin_mapping.csv`. Add the new municipality with its department.

   Re-run until it completes.
4. **Review the flags.** Check the per-year summary the extract prints for the new year against the previous years:
   - a new foreign source of financing (`Origen de Financiamiento`) has to be added to `FOREIGN_FIN_SOURCE2_CODES`;
   - a new transfer code to municipalities has to be added to `TRANSFER_ECON6_CODES`;
   - amounts out of line with earlier years may mean a change of units (municipal amounts are expected in millions).
5. **Run the DLT pipeline** and compare the gold totals by year, `func` and `econ` with the previous run. Earlier
   years should be unchanged unless the ministry revised them. The DLT tags on code prefixes, except for two
   label matches worth checking in the new year:
   - FUNCTION2 `390 - otros servicios sociales` (Social protection);
   - the `Departamento` values `Alcance Nacional`, `Auxiliar Traspaso` and `No Disponible`, which become
     `Central Scope`. A new non-geographic value would show up as a department in `geo1`.
