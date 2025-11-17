# app_v2.py - Versió Adaptada per SQLite (Log Modificat)

import streamlit as st
import pandas as pd
import json
import os
import sqlite3
from datetime import datetime, date
from pathlib import Path
import matplotlib.pyplot as plt
import subprocess
import sys
import plotly.express as px
from typing import Optional, Tuple, Dict
from collections import Counter

# --- Funcions d'Utilitat ---

def parse_user_date(s: str) -> Optional[date]:
    """Parseja una cadena d'entrada per a la CLI de main.py."""
    if not s:
        return None
    for fmt in ('%Y-%m-%d', '%d/%m/%Y'):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    raise ValueError(f"Format de data no vàlid: {s}")

def get_latest_files() -> Dict[str, Path]:
    """Troba els fitxers més recents generats (Sortida CSV/JSON)."""
    files = {}

    patterns = {
        'assignacions_json': 'assignacions_*.json',
        'assignacions_csv': 'assignacions_*.csv',
        'estadistiques': 'estadistiques_treballadors_*.csv',
        'no_cobertes': 'no_cobertes_*.csv',
        'historic_csv': 'historic_assignacions.csv' # El fitxer de backup
    }

    for key, pattern in patterns.items():
        matching_files = list(Path.cwd().glob(pattern))
        if matching_files:
            # Per l'històric, usem el fitxer directament (no el més recent)
            if key == 'historic_csv':
                 files[key] = matching_files[0] if matching_files else None
            else:
                 files[key] = max(matching_files, key=os.path.getctime)

    return files

@st.cache_data(ttl=60) # Cache per un minut
def check_input_files() -> Dict[str, Dict]:
    """Comprova si existeix la base de dades SQLite i els fitxers CSV d'entrada (si encara es necessiten per a la creació de la DB)."""
    db_path = 'treballadors.db'
    status = {}

    # Comprovació CRÍTICA: La DB
    exists = os.path.exists(db_path)
    size = os.path.getsize(db_path) / 1024 if exists else 0
    status[db_path] = {'exists': exists, 'size': size}

    # Comprovació de fitxers de referència per si es necessita regenerar la DB
    csv_ref = ['serveis.csv', 'calendari.csv', 'treballadors.csv', 'cobertura.csv']
    for file in csv_ref:
        exists_csv = os.path.exists(file)
        size_csv = os.path.getsize(file) / 1024 if exists_csv else 0
        status[f'Ref: {file}'] = {'exists': exists_csv, 'size': size_csv}

    return status

def run_system(start_date: Optional[str] = None, end_date: Optional[str] = None, on_duplicate: Optional[str] = None):
    """Executa main.py amb opcions de data opcionals i gestió de solapaments."""
    try:
        cmd = [sys.executable, 'main.py']
        if start_date:
            cmd += ['--start-date', start_date]
        if end_date:
            cmd += ['--end-date', end_date]
        if on_duplicate:
            cmd += ['--on-duplicate', on_duplicate]

        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        env['PYTHONUTF8'] = '1'
        env['PYTHONLEGACYWINDOWSSTDIO'] = 'utf-8' 

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            # MODIFICACIÓ: Fusionar stderr amb stdout per evitar el deadlock
            stderr=subprocess.STDOUT, 
            text=True,
            encoding='utf-8',
            bufsize=1,
            env=env
        )

        output = []
        # Ara process.stdout conté stdout i stderr
        for line in process.stdout: 
            cleaned_line = line.encode('utf-8', errors='ignore').decode('utf-8')
            output.append(cleaned_line)
            yield cleaned_line

        process.wait()

        if process.returncode != 0:
            # Ja no necessitem llegir process.stderr, ja s'ha llegit en el bucle
            yield "\n❌ ERROR: El procés ha acabat amb un codi de retorn diferent de zero. Revisa el log."
        else:
            yield "\n✅ Procés completat!"

    except Exception as e:
        yield f"\n❌ Error executant: {str(e)}"

# --- Configuració de la Pàgina ---
st.set_page_config(
    page_title="Assignació Treballadors",
    page_icon="🚇",
    layout="wide"
)

st.title("🚇 GA Assignacions v1 (SQLite)")
st.markdown("---")

# --- SIDEBAR ---
with st.sidebar:
    st.header("📁 Estat del Sistema")

    if st.button("🔄 Actualitzar Estat", help="Torna a comprovar la base de dades i els resultats.", use_container_width=True):
        st.cache_data.clear()
        st.experimental_rerun()

    st.markdown("### Font de Dades (CRÍTICA)")

    input_status = check_input_files()
    db_path = 'treballadors.db'

    if input_status[db_path]['exists']:
        st.success(f"✅ **{db_path}** ({input_status[db_path]['size']:.1f} KB)")
        all_exist = True
    else:
        st.error(f"❌ **{db_path}**")
        st.warning("El sistema NO pot funcionar sense `treballadors.db`.")
        all_exist = False

    st.markdown("---")

    st.markdown("### Fitxers de Resultats")

    files = get_latest_files()

    if files:
        for key, filepath in files.items():
            if filepath:
                mod_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                st.info(f"📄 **{filepath.name}**\n\n{mod_time.strftime('%d/%m/%Y %H:%M')}")
    else:
        st.warning("Cap resultat generat encara.")

    st.markdown("---")
    st.caption("Base de dades: SQLite | Sortida: JSON/CSV")

# --- PESTANYES PRINCIPALS ---
tab1, tab2, tab3, tab4 = st.tabs(["▶️ Executar", "📊 Resultats", "📈 Estadístiques", "📥 Descarregar"])

# ==================== TAB 1: EXECUTAR ====================
with tab1:
    st.header("▶️ Executar Sistema")

    if not all_exist:
        st.error("❌ **El sistema no pot connectar-se a la base de dades.** Assegura't que `treballadors.db` es troba al mateix directori.")
        st.stop()
    else:
        st.success("✅ Connexió a `treballadors.db` establerta. Llest per executar.")

        st.markdown("""
        ### 🚀 Llançar Procés d'Assignació
        **Nota:** Les dades de treballadors, torns i cobertura es llegeixen **directament de la DB SQLite.**
        """)

        # Controls per a filtrar per dates abans d'executar (selector de dates)
        with st.expander("Filtre per Interval de Dates (Opcional)", expanded=False):
            enable_filter = st.checkbox("Activar filtre per dates", key="enable_date_filter")

            start_input: Optional[date] = None
            end_input: Optional[date] = None

            if enable_filter:
                col_s, col_e = st.columns(2)
                with col_s:
                    start_input = st.date_input("Data d'inici", value=None, key="start_date_input")
                with col_e:
                    end_input = st.date_input("Data final", value=None, key="end_date_input")

        # Opció per a gestionar dates solapades amb l'històric
        st.markdown("### Gestió d'Assignacions Prèvies")

        on_dup_options = {
            'Actualitzar i eliminar prèvies': 'replace_all',
            'Afegir noves només (excloure treballadors amb assignació prèvia)': 'add_new_only'
        }

        selected_option = st.radio(
            'Tria com gestionar assignacions prèvies (Històric vs. Necessitats actuals)',
            options=list(on_dup_options.keys()),
            index=0
        )

        on_duplicate_value = on_dup_options[selected_option]

        # Botó d'execució
        if st.button("▶️ EXECUTAR MAIN.PY", type="primary", use_container_width=True):
            sd_iso: Optional[str] = None
            ed_iso: Optional[str] = None

            if enable_filter:
                if start_input is None and end_input is None:
                    st.error("Has activat el filtre per dates però no has triat cap data.")
                    st.stop()

                sd_date, ed_date = start_input, end_input

                if sd_date and ed_date and sd_date > ed_date:
                    sd_date, ed_date = ed_date, sd_date
                    st.info("S'han intercanviat les dates.")

                sd_iso = sd_date.isoformat() if sd_date else None
                ed_iso = ed_date.isoformat() if ed_date else None

            with st.status("Executant Algorisme Genètic...", expanded=True) as status_container:

                # MODIFICACIÓ CLAU: Utilitzem st.empty() i st.code per simular la consola
                log_placeholder = st.empty() 
                log_text = ""

                for line in run_system(start_date=sd_iso, end_date=ed_iso, on_duplicate=on_duplicate_value):
                    log_text += line
                    # Actualitzem el placeholder reescrivint tot el log_text
                    log_placeholder.code(log_text, language='text') 

                # Després que el procés acaba, reemplacem el placeholder pel missatge final dins del status
                if "✅ Procés completat!" in log_text:
                    status_container.update(label="✅ Execució completada!", state="complete", expanded=False)
                    st.success("✅ Execució completada! Revisa les altres pestanyes per veure els resultats.")
                    st.balloons()
                    st.cache_data.clear() 
                    st.rerun()
                else:
                    status_container.update(label="❌ Error durant l'execució", state="error", expanded=True)
                    st.error("❌ Hi ha hagut un error. Revisa el log detallat anterior.")
                    # Mantenim el log final si hi ha hagut error
                    log_placeholder.code(log_text, language='text')


# ==================== TAB 2: RESULTATS ====================
with tab2:
    st.header("📊 Resultats de l'Assignació")

    files = get_latest_files()

    if not files.get('assignacions_json'):
        st.warning("⚠️ No hi ha resultats disponibles. Executa el sistema primer.")
    else:
        try:
            with open(files['assignacions_json'], 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            st.error(f"Error carregant el fitxer de resultats: {e}")
            st.stop()

        metadata = data.get('metadata', {})
        scores = data.get('scores_restriccions', {})
        assignacions = data.get('assignacions', [])

        # Mètriques principals
        st.subheader("Resum d'Execució")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Score Total", f"{metadata.get('score_total', 0):.2f}/100")
        with col2:
            st.metric("Total Assignacions", metadata.get('total_assignacions', 0))
        with col3:
            cobertura = metadata.get('cobertura_percentatge', 0)
            st.metric("Cobertura", f"{cobertura:.1f}%", delta="Completa" if cobertura >= 100 else f"Falta {100 - cobertura:.1f}%")
        with col4:
            st.metric("Treballadors Utilitzats", metadata.get('treballadors_utilitzats', 0))

        st.markdown("---")

        # Scores de restriccions
        st.subheader("🎯 Scores per Restricció")

        if scores:
            df_scores = pd.DataFrame([
                {'Restricció': nom, 'Score': info.get('score', 0), 'Pes': info.get('pes', 0)}
                for nom, info in scores.items()
            ])
            df_scores['Contribució'] = df_scores['Score'] * df_scores['Pes']
            df_scores = df_scores.sort_values('Contribució', ascending=False)

            df_scores['Tipus'] = df_scores['Pes'].apply(lambda p: 'Crítica (>= 0.10)' if p >= 0.10 else ('Important (>= 0.03)' if p >= 0.03 else 'Equitat (< 0.03)'))

            fig = px.bar(
                df_scores, x='Score', y='Restricció', orientation='h', color='Tipus',
                color_discrete_map={'Crítica (>= 0.10)': 'red', 'Important (>= 0.03)': 'orange', 'Equitat (< 0.03)': 'green'},
                range_x=[0, 100], title="Scores de Restriccions"
            )
            fig.update_layout(height=450)
            st.plotly_chart(fig, use_container_width=True)

            st.dataframe(
                df_scores[['Restricció', 'Score', 'Pes', 'Contribució']].style.background_gradient(subset=['Score'], cmap='RdYlGn', vmin=0, vmax=100),
                use_container_width=True, hide_index=True
            )

        st.markdown("---")

        # Taula d'assignacions
        st.subheader("📋 Taula Detallada d'Assignacions")

        if assignacions:
            df_assign = pd.DataFrame(assignacions)
            df_assign['data'] = pd.to_datetime(df_assign['data'])

            st.dataframe(
                df_assign[[
                    'data', 'dia_setmana', 'treballador_nom', 'torn', 
                    'hora_inici', 'hora_fi', 'durada_hores', 'zona', 'formacio', 
                    'es_canvi_zona', 'es_canvi_torn'
                ]].sort_values('data'),
                use_container_width=True, hide_index=True,
                column_config={"data": st.column_config.DateColumn("Data", format="YYYY-MM-DD")}
            )
            st.info(f"Mostrant {len(df_assign)} assignacions.")


# ==================== TAB 3: ESTADÍSTIQUES ====================
with tab3:
    st.header("📈 Estadístiques de Treballadors")

    files = get_latest_files()

    if not files.get('estadistiques'):
        st.warning("⚠️ No hi ha estadístiques disponibles.")
    else:
        try:
            df_stats = pd.read_csv(files['estadistiques'], encoding='utf-8-sig')
            df_stats['Hores_Totals_Any'] = pd.to_numeric(df_stats['Hores_Totals_Any'], errors='coerce')
        except Exception as e:
            st.error(f"Error carregant el fitxer d'estadístiques: {e}")
            st.stop()

        # Resum general
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Treballadors Actius", len(df_stats))
        with col2:
            st.metric("Total Hores (Període)", f"{df_stats['Hores_Periode'].sum():.1f}h")
        with col3:
            st.metric("Mitjana Assignacions/Treb.", f"{df_stats['Assignacions_Periode'].mean():.1f}")

        st.markdown("---")

        # Gràfic de distribució
        st.subheader("📊 Distribució d'Assignacions")
        top_n = st.slider("Mostrar Top N Treballadors", min_value=5, max_value=len(df_stats), value=15)
        df_top = df_stats.sort_values('Assignacions_Periode', ascending=False).head(top_n)

        fig = px.bar(df_top, x='Nom', y='Assignacions_Periode', hover_data=['Hores_Totals_Any'], title=f"Top {top_n} Treballadors per Assignacions", color='Assignacions_Periode', color_continuous_scale='Blues')
        fig.update_layout(height=400, xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Alertes i Taula
        st.subheader("⚠️ Alertes d'Hores Anuals")

        df_limit_alt = df_stats[df_stats['Hores_Totals_Any'] > 1100].sort_values('Hores_Totals_Any', ascending=False)

        if not df_limit_alt.empty:
            st.warning(f"⚠️ **{len(df_limit_alt)} treballador(s)** amb més de 1.100h anuals.")
            for _, row in df_limit_alt.iterrows():
                hores = row['Hores_Totals_Any']
                nom = row['Nom']
                if hores > 1605:
                    st.error(f"🚨 **{nom}**: {hores:.1f}h (SUPERANT el màxim de 1.605h!)")
                elif hores > 1218:
                    st.warning(f"🟠 **{nom}**: {hores:.1f}h (Supera el límit estàndard de 1.218h)")
        else:
            st.success("✅ Tots els treballadors dins dels límits raonables d'hores.")

        st.markdown("---")
        st.subheader("📋 Detall Complet per Treballador")

        st.dataframe(
            df_stats[['Nom', 'Grup', 'Assignacions_Periode', 'Hores_Periode', 'Hores_Totals_Any', 'Hores_Disponibles', 'Canvis_Zona_Total', 'Canvis_Torn_Total']].sort_values('Hores_Totals_Any', ascending=False),
            use_container_width=True, hide_index=True
        )


# ==================== TAB 4: DESCARREGAR ====================
with tab4:
    st.header("📥 Descarregar Fitxers de Resultats")

    files = get_latest_files()

    if not files:
        st.warning("⚠️ No hi ha fitxers disponibles per descarregar.")
    else:
        st.markdown("### Fitxers Generats Individualment (CSV/JSON)")
        st.markdown("---")

        for key, filepath in files.items():
            if filepath:
                try:
                    with open(filepath, 'rb') as f:
                        file_bytes = f.read()
                except Exception:
                    continue

                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown(f"**{filepath.name}** 💾 {len(file_bytes) / 1024:.1f} KB")

                with col2:
                    st.download_button(
                        label="⬇️ Descarregar", data=file_bytes, file_name=filepath.name, 
                        mime='application/octet-stream', key=f"download_{key}", use_container_width=True
                    )
                st.markdown("---")

        # Descarregar tots en ZIP
        st.markdown("### 📦 Descarregar Tot en ZIP")
        if st.button("📦 Generar ZIP amb tots els fitxers de sortida", use_container_width=True):
            import zipfile
            from io import BytesIO
            zip_buffer = BytesIO()
            with st.spinner("Generant fitxer ZIP..."):
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    for filepath in files.values():
                        if filepath and os.path.exists(filepath):
                            zip_file.write(filepath, filepath.name)
            zip_buffer.seek(0)
            st.download_button(
                label="⬇️ Descarregar resultats.zip", data=zip_buffer, file_name=f"resultats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip", 
                mime='application/zip', use_container_width=True
            )
            st.success("ZIP generat correctament.")

# --- FOOTER ---
st.markdown("---")
st.caption("🚇 Sistema d'Assignació de Treballadors v1.1 | Adaptat a SQLite")