import json
import csv
import re
from pathlib import Path
from lxml import etree
from jsonpath_ng.ext import parse
from collections import defaultdict
import pandas as pd



# === CONFIG ===
OPT_PATH = Path("eligibility.opt")
INSTANCES_DIR = Path("instances/")
CSV_AGREGADO = Path("jsonpath_aggregated_results.csv")
TABLAS_DIR = Path("tablas_por_arquetipo")
EXCEL_SALIDA = Path("informe_calidad.xlsx")
TABLAS_DIR.mkdir(exist_ok=True)

# === NAMESPACES y tipos ===
NAMESPACES = {
    "default": "http://schemas.openehr.org/v1",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance"
}

DV_TYPE_ATTRIBUTES = {
    "DV_TEXT": ["value"],
    "DV_CODED_TEXT": ["value", "defining_code.code_string", "defining_code.terminology_id.value"],
    "DV_COUNT": ["magnitude"],
    "DV_QUANTITY": ["magnitude", "units"],
    "DV_DATE": ["value"],
    "DV_DATE_TIME": ["value"],
    "DV_BOOLEAN": ["value"],
    "DV_ORDINAL": ["value", "symbol.value"],
    "DV_SCALE": ["value", "symbol.value"]
}

# === PARTE 1: .opt → JSONPaths enriquecidos ===

def load_opt(file_path):
    with open(file_path, "rb") as f:
        return etree.parse(f)


def extract_local_term_definitions(c_root):
    all_terms = {}

    # Términos de arquetipos embebidos
    for block in c_root.findall(".//default:children[@xsi:type='C_ARCHETYPE_ROOT']", namespaces=NAMESPACES):
        local_id_elem = block.find("default:archetype_id/default:value", namespaces=NAMESPACES)
        local_id = local_id_elem.text if local_id_elem is not None else None
        #print(etree.tostring(block, pretty_print=True).decode())
        term_defs = block.findall(".//default:term_definitions", namespaces=NAMESPACES)
        for defs in term_defs:
            #print(etree.tostring(defs, pretty_print=True).decode())
            code = defs.get("code")
            for item in defs.findall("default:items", namespaces=NAMESPACES):
                if item.get("id") == "text" and item.text:
                    all_terms[f"{local_id}:{code}"] = item.text.strip()


    # Términos del arquetipo raíz
    archetype_id_elem = c_root.find("default:archetype_id/default:value", namespaces=NAMESPACES)
    root_id = archetype_id_elem.text if archetype_id_elem is not None else "UNKNOWN"
    for defs in c_root.findall("default:term_definitions", namespaces=NAMESPACES):
        for item in defs.findall("default:items", namespaces=NAMESPACES):
            code = defs.get("code")
            for item in defs.findall("default:items", namespaces=NAMESPACES):
                if item.get("id") == "text" and item.text:
                    all_terms[f"{root_id}:{code}"] = item.text.strip()

    with open("debug_term_map.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["archetype_id:node_id", "label"])
        for k, v in all_terms.items():
            writer.writerow([k, v])

    return root_id, all_terms


def build_jsonpaths(node, current_path, term_map, archetype_id, is_root=False):
    results = []

    node_id = node.findtext("default:node_id", namespaces=NAMESPACES)
    if node_id is None:
        return results

    node_type = node.attrib.get(f"{{{NAMESPACES['xsi']}}}type")
    local_archetype_id = None
    if node_type == "C_ARCHETYPE_ROOT":
        local_archetype_id = node.findtext("default:archetype_id/default:value", namespaces=NAMESPACES)

    # Construcción del path
    if is_root:
        ref_id = archetype_id
        path_predicate = f"[?(@.archetype_node_id=='{ref_id}')]"
    elif node_id == "at0000" and local_archetype_id:
        ref_id = local_archetype_id
        path_predicate = f"[?(@.archetype_details.archetype_id.value=='{ref_id}')]"
    else:
        ref_id = node_id
        path_predicate = f"[?(@.archetype_node_id=='{ref_id}')]"

    parent = node.getparent()
    if parent is not None and parent.tag.endswith("attributes"):
        attr_name = parent.findtext("default:rm_attribute_name", namespaces=NAMESPACES)
        xsi_type = parent.attrib.get(f"{{{NAMESPACES['xsi']}}}type", "")
        is_multiple = (xsi_type == "C_MULTIPLE_ATTRIBUTE")
        segment = f".{attr_name}{path_predicate}" if is_multiple else f".{attr_name}"
        current_path += segment
    elif is_root:
        current_path += path_predicate

    # Determinación del archetype_id al que pertenece el nodo actual
    current_archetype = local_archetype_id if local_archetype_id else archetype_id
    readable_label = term_map.get(f"{current_archetype}:{node_id}", "")

    results.append((current_path, readable_label))

    if node.attrib.get(f"{{{NAMESPACES['xsi']}}}type") == "C_COMPLEX_OBJECT":
        rm_type = node.findtext("default:rm_type_name", namespaces=NAMESPACES)
        if rm_type == "ELEMENT":
            value_path = current_path + ".value"
            value_label = f"{readable_label}.value" if readable_label else "value"
            results.append((value_path, value_label))
            for attrs in DV_TYPE_ATTRIBUTES.values():
                for attr in attrs:
                    sub_path = f"{value_path}.{attr}"
                    sub_label = f"{value_label}.{attr}" if value_label else attr
                    results.append((sub_path, sub_label))

    # Recursión
    for attribute in node.findall("default:attributes", namespaces=NAMESPACES):
        for child in attribute.findall("default:children", namespaces=NAMESPACES):
            next_archetype_id = local_archetype_id if local_archetype_id else archetype_id
            results.extend(build_jsonpaths(child, current_path, term_map, next_archetype_id, is_root=False))

    return results



def extract_jsonpaths_from_opt(tree):
    root = tree.getroot()
    #archetype_roots = root.xpath(".//default:children[@xsi:type='C_ARCHETYPE_ROOT']", namespaces=NAMESPACES)
    archetype_roots = root.xpath("./default:definition/default:attributes/default:children[@xsi:type='C_ARCHETYPE_ROOT']", namespaces=NAMESPACES)
    all_paths = []
    for c_root in archetype_roots:
        archetype_id, terms = extract_local_term_definitions(c_root)
        all_paths.extend(build_jsonpaths(c_root, "$", terms, archetype_id, is_root=True))
    return all_paths

# === PARTE 2: Evaluación JSONPath sobre múltiples JSON ===

def evaluate_jsonpaths(jsonpaths, json_files):
    aggregated = {}
    for path_expr, label in jsonpaths:
        try:
            parsed_expr = parse(path_expr)
        except Exception as e:
            aggregated[path_expr] = {
                "jsonpath": path_expr, "label": label, "values": "",
                "empty_percentage": 100.0, "unique_value_percentage": 0.0,
                "file_count": len(json_files), "error": str(e)
            }
            continue

        empty_count = 0
        simple_values = []
        for json_file in json_files:
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                matches = [m.value for m in parsed_expr.find(data)]
                if not matches:
                    empty_count += 1
                else:
                    for m in matches:
                        if isinstance(m, (str, int, float, bool, type(None))):
                            simple_values.append(str(m))
            except Exception:
                empty_count += 1

        freq_counter = defaultdict(int)
        for val in simple_values:
            freq_counter[val] += 1

        values_summary = json.dumps([{k: v} for k, v in freq_counter.items()], ensure_ascii=False) if freq_counter else ""
        unique_count = len(freq_counter)


        try:
            values_summary = sorted(values_summary, key=lambda d: -list(d.values())[0])
        except Exception:
            pass  # Si falla el parseo, lo dejamos como está
	
        aggregated[path_expr] = {
            "jsonpath": path_expr, "label": label, "values": values_summary,
            "empty_percentage": (empty_count / len(json_files)) * 100,
            "unique_value_percentage": 0.0 if not simple_values else (unique_count / len(simple_values)) * 100,
            "file_count": len(json_files), "error": ""
        }
    return aggregated

def write_csv(csv_path, results):
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "jsonpath", "label", "values", "empty_percentage",
            "unique_value_percentage", "file_count", "error"
        ])
        writer.writeheader()
        for row in results.values():
            writer.writerow(row)

# === PARTE 3: Tablas por arquetipo pivotadas ===

def extract_arquetipo(jsonpath):
    match = re.search(r"@\.archetype_details\.archetype_id\.value=='(openEHR-EHR-[^']+)'", jsonpath)
    return match.group(1) if match else "UNKNOWN"

def generar_tablas_por_arquetipo(csv_path):
    arquetipo_map = defaultdict(list)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row["values"]:
                continue
            arquetipo = extract_arquetipo(row["jsonpath"])
            try:
                values = json.loads(row["values"])
                label = row["label"]
                arquetipo_map[arquetipo].append((label, values))
            except json.JSONDecodeError:
                continue

    for arquetipo, columnas_y_valores in arquetipo_map.items():
        columnas = []
        filas = []
        max_len = max(len(vals) for _, vals in columnas_y_valores)
        for i in range(max_len):
            fila = []
            for label, valores in columnas_y_valores:
                if i == 0:
                    columnas.extend([label, "frecuencia"])
                if i < len(valores):
                    val_dict = valores[i]
                    val, freq = list(val_dict.items())[0]
                    fila.extend([val, freq])
                else:
                    fila.extend(["", ""])
            filas.append(fila)

        salida = TABLAS_DIR / f"tabla_{arquetipo.replace('.', '_')}.csv"
        with open(salida, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columnas)
            writer.writerows(filas)

# === PARTE 4: Exportar Excel ===

def exportar_excel():
    df_principal = pd.read_csv(CSV_AGREGADO)

    if "values" in df_principal.columns:
        df_principal.drop(columns=["values"], inplace=True)

    with pd.ExcelWriter(EXCEL_SALIDA, engine="openpyxl") as writer:
        df_principal.to_excel(writer, index=False, sheet_name="Resumen general")

        for csv_file in TABLAS_DIR.glob("tabla_*.csv"):
            nombre = csv_file.stem.replace("tabla_", "")[:31]
            try:
                df = pd.read_csv(csv_file, dtype=str)
                nuevas_columnas = []
                for col in df.columns:
                    if re.match(r"^frecuencia(\.\d+)?$", col):
                        nuevas_columnas.append("frecuencia")
                    else:
                        nuevas_columnas.append(col)
                df.columns = nuevas_columnas
                df.to_excel(writer, index=False, sheet_name=nombre)
            except Exception as e:
                print(f"❌ Error en hoja {csv_file.name}: {e}")



# === MAIN ===

if __name__ == "__main__":
    print("📥 Cargando .opt...")
    opt_tree = load_opt(OPT_PATH)

    print("🔍 Generando JSONPaths enriquecidos...")
    jsonpaths = extract_jsonpaths_from_opt(opt_tree)

    print(f"📁 Buscando ficheros JSON en: {INSTANCES_DIR}")
    json_files = list(INSTANCES_DIR.glob("*.json"))

    print(f"🧠 Evaluando {len(jsonpaths)} JSONPaths sobre {len(json_files)} ficheros...")
    results = evaluate_jsonpaths(jsonpaths, json_files)

    print(f"💾 Escribiendo CSV principal: {CSV_AGREGADO}")
    write_csv(CSV_AGREGADO, results)

    print("📊 Generando tablas por arquetipo...")
    generar_tablas_por_arquetipo(CSV_AGREGADO)

    print(f"📈 Exportando a Excel: {EXCEL_SALIDA}")
    exportar_excel()

    print("✅ Proceso completado.")
