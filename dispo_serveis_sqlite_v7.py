import sqlite3
import csv
import os
from datetime import datetime, timedelta

# ============================================================================
# VERSIÓ 8: CORRECCIÓ DE LA LÒGICA D'ASSIGNACIÓ
# S'assegura que s'assigna el servei al treballador EFECTIU (Original o Substitut)
# utilitzant el seu ID per a la comprovació d'ocupació diària.
# ============================================================================

def obtenir_connexio(db_path='treballadors.db'):
    """Crea una connexió a la base de dades (treballadors.db)"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def generar_dates_interval(data_inici, data_fi):
    """Genera totes les dates de l'interval"""
    dates = []
    data_actual = data_inici
    while data_actual <= data_fi:
        dates.append(data_actual)
        data_actual += timedelta(days=1)
    return dates

def obtenir_info_treballador(db_path, treballador_id):
    """Obté la informació completa d'un treballador pel seu ID."""
    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, treballador, plaza, rotacio, zona, grup 
        FROM treballadors 
        WHERE id = ?
    ''', (treballador_id,))
    info = cursor.fetchone()
    conn.close()

    if info:
        return {
            'id': info['id'],
            'nom': info['treballador'],
            'plaza': info['plaza'],
            'rotacio': info['rotacio'],
            'zona': info['zona'],
            'grup': info['grup'],
        }
    return None

def obtenir_treballador_efectiu(db_path, treballador_id, data):
    """
    Retorna l'ID del treballador efectiu per una data donada.
    - Si l'original té substitució, retorna l'ID del substitut.
    - Si l'original té descans sense substitut, retorna None.
    - Si l'original no té descans, retorna el treballador_id original.
    """
    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()

    if isinstance(data, (datetime, type(datetime.now().date()))):
        data_str = data.strftime('%Y-%m-%d')
    else:
        data_str = data 

    cursor.execute('''
        SELECT treballador_substitut_id
        FROM descansos_dies
        WHERE treballador_id = ?
        AND data = ?
        LIMIT 1
    ''', (treballador_id, data_str))

    resultat = cursor.fetchone()
    conn.close()

    if resultat:
        if resultat['treballador_substitut_id']:
            # Lloc cobert per un substitut
            return resultat['treballador_substitut_id']
        else:
            # Lloc no cobert (descans sense substitut)
            return None 
    else:
        # No té descans, la plaça està coberta pel treballador original
        return treballador_id

def carregar_treballadors_i_descansos(db_path='treballadors.db'):
    """
    Carrega tots els treballadors. Els descansos es comproven en temps real 
    amb obtenir_treballador_efectiu per gestionar substitucions.
    """
    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()

    # Carreguem tota la info dels treballadors per la cerca ràpida per 'plaza'
    cursor.execute('SELECT id, treballador, plaza, rotacio, zona, grup FROM treballadors')
    treballadors = {}

    for row in cursor.fetchall():
        plaza = row['plaza']
        treballadors[plaza] = {
            'id': row['id'],
            'nom': row['treballador'],
            'plaza': plaza,
            'rotacio': row['rotacio'],
            'zona': row['zona'],
            'grup': row['grup'],
        }

    conn.close()

    return treballadors

def carregar_serveis(db_path='treballadors.db'):
    """Carrega els serveis de la base de dades"""
    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM serveis')
    serveis = []

    for row in cursor.fetchall():
        servei_dict = {
            'servei': row['servei'],
            'opció_1': row['opcio_1'],
            'opció_2': row['opcio_2']
        }

        # Afegir columnes opcionals si existeixen
        try:
            servei_dict['rotacio'] = row['rotacio']
        except (KeyError, IndexError):
            try:
                servei_dict['rotacio'] = row['torn']
            except (KeyError, IndexError):
                servei_dict['rotacio'] = None

        try:
            servei_dict['formacio'] = row['formacio']
        except (KeyError, IndexError):
            servei_dict['formacio'] = None

        try:
            servei_dict['linia'] = row['linia']
        except (KeyError, IndexError):
            servei_dict['linia'] = None

        try:
            servei_dict['zona'] = row['zona']
        except (KeyError, IndexError):
            servei_dict['zona'] = None

        serveis.append(servei_dict)

    conn.close()
    return serveis

def netejar_taules(db_path='treballadors.db'):
    """
    Neteja COMPLETAMENT les taules cobertura i assig_grup_A (totes les línies)
    """
    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()

    # Esborrar TOTES les línies de la taula assig_grup_A
    cursor.execute('DELETE FROM assig_grup_A')
    registres_assig = cursor.rowcount

    # Esborrar TOTES les línies de la taula cobertura
    cursor.execute('DELETE FROM cobertura')
    registres_cobertura = cursor.rowcount

    conn.commit()
    conn.close()

    print(f"\n🧹 Neteja COMPLETA realitzada:")
    print(f"   · {registres_assig} registres esborrats de 'assig_grup_A' (TOTS)")
    print(f"   · {registres_cobertura} registres esborrats de 'cobertura' (TOTS)")

def guardar_assignacions_db(assignacions_per_dia, db_path='treballadors.db'):
    """Guarda les assignacions a la base de dades"""
    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()

    # Inserir noves assignacions
    for data_actual, assignacions in assignacions_per_dia.items():
        data_str = data_actual.strftime('%Y-%m-%d')

        # A. Serveis coberts -> Taula assig_grup_A
        for servei in assignacions['coberts']:
            timestamp_apunt_individual = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

            cursor.execute('''
                INSERT INTO assig_grup_A
                (servei, treballador_id, data, prioritat, estat, grup, rotacio, formacio, linia, zona, data_apunt)
                VALUES (?, ?, ?, ?, 'cobert', ?, ?, ?, ?, ?, ?)
            ''', (
                servei['servei'],
                servei['treballador_id'], # ID del treballador EFECTIU
                data_str,
                servei['prioritat'],
                servei['grup'],
                servei.get('rotacio'), # Columna rotacio
                servei.get('formacio'),
                servei.get('linia'),
                servei.get('zona'),
                timestamp_apunt_individual
            ))

        # B. Serveis descoberts -> Taula cobertura
        for servei in assignacions['descoberts']:
                cursor.execute('''
                INSERT INTO cobertura
                (servei, data, motiu_no_cobert, rotacio, formacio, linia, zona)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                servei['servei'],
                data_str,
                servei['motiu_no_cobert'],
                servei.get('rotacio'),
                servei.get('formacio'),
                servei.get('linia'),
                servei.get('zona')
            ))

    conn.commit()
    conn.close()

# ============================================================================
# PROGRAMA PRINCIPAL
# ============================================================================

print("="*80)
print("DISPONIBILITAT DE SERVEIS (amb Suport a Substitucions - v8 CORREGIDA)")
print("="*80)

db_path = 'treballadors.db'

if not os.path.exists(db_path):
    print(f"❌ Error: No s'ha trobat la base de dades '{db_path}'")
    print("   Assegura't que el fitxer treballadors.db existeix.")
    exit()

# Carregar treballadors (només info base)
print("\n📂 Carregant treballadors base...")
treballadors_per_plaza = carregar_treballadors_i_descansos(db_path)
print(f"✅ Total treballadors/places carregats: {len(treballadors_per_plaza)}")

# Carregar serveis
print("\n📂 Carregant serveis...")
serveis = carregar_serveis(db_path)
print(f"✅ Total serveis carregats: {len(serveis)}")

# Demanar interval de temps a l'usuari
print("\n🔍 INTRODUIR INTERVAL DE TEMPS PER COMPROVAR DESCANSOS")
print("Format de data: YYYY-MM-DD")
data_inici_str = input("Data d'inici (YYYY-MM-DD): ")
data_fi_str = input("Data de fi (YYYY-MM-DD): ")

try:
    data_inici = datetime.strptime(data_inici_str, '%Y-%m-%d').date()
    data_fi = datetime.strptime(data_fi_str, '%Y-%m-%d').date()
except ValueError:
    print("❌ Format de data incorrecte. Utilitza YYYY-MM-DD")
    exit()

dates_interval = generar_dates_interval(data_inici, data_fi)
print(f"\n📅 Analitzant {len(dates_interval)} dies del {data_inici.strftime('%Y-%m-%d')} al {data_fi.strftime('%Y-%m-%d')}")

assignacions_per_dia = {data: {'coberts': [], 'descoberts': []} for data in dates_interval}
# Guardarem l'ID del treballador EFECTIU assignat per evitar duplicats
treballadors_ocupats_per_dia = {data: set() for data in dates_interval} 

# Processar cada dia
for data_actual in dates_interval:
    print(f"\n🔍 Processant dia {data_actual.strftime('%Y-%m-%d')}...")
    serveis_coberts_avui = []
    serveis_descoberts_avui = []

    for servei in serveis:
        id_efectiu = None
        treballador_efectiu_info = None
        plaza_original_trobada = None
        prioritat = None
        motiu_no_cobert = "No trobat"

        # 1. Comprovar opció_1 (PRIMERA PRIORITAT)
        plaza_original_1 = servei['opció_1']
        if plaza_original_1 in treballadors_per_plaza:
            plaza_original_id = treballadors_per_plaza[plaza_original_1]['id']

            # Obté l'ID del treballador efectiu (original o substitut) o None
            id_efectiu_1 = obtenir_treballador_efectiu(db_path, plaza_original_id, data_actual)

            if id_efectiu_1 is not None:
                # Comprovem si el treballador efectiu ja està assignat a un altre servei avui (CRÍTIC: ID)
                if id_efectiu_1 not in treballadors_ocupats_per_dia[data_actual]:
                    id_efectiu = id_efectiu_1
                    plaza_original_trobada = plaza_original_1
                    prioritat = "opció_1"
                    motiu_no_cobert = None
                else:
                    motiu_no_cobert = f"Treballador efectiu (ID {id_efectiu_1}) ja ocupat"
            else:
                motiu_no_cobert = "Opció 1 té descans sense substitut"
        elif plaza_original_1:
             motiu_no_cobert = "Opció 1 (Plaça) no trobada a la DB"

        # 2. Si no troba, buscar per opció_2 (SEGONA PRIORITAT)
        if id_efectiu is None:
            plaza_original_2 = servei['opció_2']
            motiu_no_cobert_op2 = "No trobat" # Mantenim el missatge d'error de l'Opció 1 si falla

            if plaza_original_2 in treballadors_per_plaza:
                plaza_original_id = treballadors_per_plaza[plaza_original_2]['id']

                id_efectiu_2 = obtenir_treballador_efectiu(db_path, plaza_original_id, data_actual)

                if id_efectiu_2 is not None:
                    # Comprovem si el treballador efectiu ja està assignat (CRÍTIC: ID)
                    if id_efectiu_2 not in treballadors_ocupats_per_dia[data_actual]:
                        id_efectiu = id_efectiu_2
                        plaza_original_trobada = plaza_original_2
                        prioritat = "opció_2"
                        motiu_no_cobert = None
                    else:
                        motiu_no_cobert_op2 = f"Treballador efectiu (ID {id_efectiu_2}) ja ocupat"
                else:
                    motiu_no_cobert_op2 = "Opció 2 té descans sense substitut"
            elif plaza_original_2:
                 motiu_no_cobert_op2 = "Opció 2 (Plaça) no trobada a la DB"

            # Si l'Opció 2 no ha cobert, actualitzem el motiu amb el missatge de la Opció 2
            if id_efectiu is None and plaza_original_2:
                 motiu_no_cobert = motiu_no_cobert_op2


        # 3. Guardar resultat
        if id_efectiu is not None:
            # Obtenir la informació completa del treballador efectiu
            treballador_efectiu_info = obtenir_info_treballador(db_path, id_efectiu)

            if treballador_efectiu_info:
                servei_assignat = servei.copy()
                servei_assignat['treballador'] = treballador_efectiu_info['nom']
                servei_assignat['treballador_id'] = treballador_efectiu_info['id'] # ID del treballador EFECTIU
                servei_assignat['plaza_trobada'] = plaza_original_trobada # Plaza ORIGINAL que cobreix
                servei_assignat['prioritat'] = prioritat
                servei_assignat['data'] = data_actual.strftime('%Y-%m-%d')

                # Usem la info del treballador EFECTIU (Substitut o Original)
                servei_assignat['grup'] = treballador_efectiu_info['grup'] 
                servei_assignat['rotacio'] = treballador_efectiu_info['rotacio'] # Utilitzem la rotació del treballador, no la del servei
                servei_assignat['zona'] = treballador_efectiu_info['zona']

                # Mantenim la info del servei si no la teníem al treballador
                servei_assignat['formacio'] = servei.get('formacio')
                servei_assignat['linia'] = servei.get('linia')

                serveis_coberts_avui.append(servei_assignat)
                # Marquem el treballador EFECTIU com a ocupat
                treballadors_ocupats_per_dia[data_actual].add(id_efectiu) 
            else:
                 # Això no hauria de passar si la DB és coherent
                motiu_no_cobert = f"Treballador efectiu (ID {id_efectiu}) no trobat"

                servei_no_cobert = servei.copy()
                servei_no_cobert['motiu_no_cobert'] = motiu_no_cobert
                servei_no_cobert['data'] = data_actual.strftime('%Y-%m-%d')
                serveis_descoberts_avui.append(servei_no_cobert)
        else:
            # Si no s'ha assignat, el motiu_no_cobert ja hauria d'estar determinat
            servei_no_cobert = servei.copy()
            servei_no_cobert['motiu_no_cobert'] = motiu_no_cobert
            servei_no_cobert['data'] = data_actual.strftime('%Y-%m-%d')
            serveis_descoberts_avui.append(servei_no_cobert)

    assignacions_per_dia[data_actual]['coberts'] = serveis_coberts_avui
    assignacions_per_dia[data_actual]['descoberts'] = serveis_descoberts_avui

# Confirmació abans de guardar
print("\n" + "="*80)
print("📊 RESUM D'ASSIGNACIONS GENERADES:")
print("="*80)

resum_coberts = []
resum_descoberts = []

for data_actual in dates_interval:
    resum_coberts.extend(assignacions_per_dia[data_actual]['coberts'])
    resum_descoberts.extend(assignacions_per_dia[data_actual]['descoberts'])

print(f"📅 Interval: {data_inici_str} a {data_fi_str}")
print(f"📋 Total serveis coberts: {len(resum_coberts)}")
print(f"📋 Total serveis descoberts: {len(resum_descoberts)}")
print("\n⚠️  ATENCIÓ: Les taules 'assig_grup_A' i 'cobertura' s'esborraran COMPLETAMENT abans de guardar.")
print("="*80)

confirmacio = input("\n💾 Vols guardar aquestes assignacions a la base de dades? (S/N): ").strip().upper()

if confirmacio == 'S':
    print("\n🧹 Netejant TOTES les línies de les taules...")
    netejar_taules(db_path)

    print("\n💾 Guardant assignacions a la base de dades...")
    guardar_assignacions_db(assignacions_per_dia, db_path)
    print("✅ Assignacions guardades correctament a la base de dades!")

    # També guardar a CSV per compatibilitat
    output_dir = 'dispo_serveis'
    os.makedirs(output_dir, exist_ok=True)

    # ... (Codi d'exportació a CSV mantingut sense canvis a la lògica) ...
    for data_actual in dates_interval:
        data_str = data_actual.strftime('%Y-%m-%d')

        if assignacions_per_dia[data_actual]['coberts']:
            with open(os.path.join(output_dir, f'serveis_coberts_{data_str}.csv'), 'w', newline='', encoding='utf-8') as f:
                # Obtenir la llista de camps del primer element
                fieldnames = list(assignacions_per_dia[data_actual]['coberts'][0].keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(assignacions_per_dia[data_actual]['coberts'])

        if assignacions_per_dia[data_actual]['descoberts']:
            with open(os.path.join(output_dir, f'serveis_descoberts_{data_str}.csv'), 'w', newline='', encoding='utf-8') as f:
                fieldnames = list(assignacions_per_dia[data_actual]['descoberts'][0].keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(assignacions_per_dia[data_actual]['descoberts'])

    # Guardar resums
    if resum_coberts:
        with open(os.path.join(output_dir, 'resum_serveis_coberts.csv'), 'w', newline='', encoding='utf-8') as f:
            fieldnames = list(resum_coberts[0].keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(resum_coberts)

    if resum_descoberts:
        with open(os.path.join(output_dir, 'resum_serveis_descoberts.csv'), 'w', newline='', encoding='utf-8') as f:
            fieldnames = list(resum_descoberts[0].keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(resum_descoberts)

    print(f"\n💾 Resultats guardats:")
    print(f"   - Base de dades: {db_path}")
    print(f"     · Taula 'assig_grup_A' (serveis coberts)")
    print(f"     · Taula 'cobertura' (serveis descoberts)")
    print(f"   - CSVs a carpeta: {output_dir}/")
else:
    print("\n❌ Operació cancel·lada. No s'ha guardat res a la base de dades.")

print("\n" + "="*80)
print("PROCÉS FINALITZAT")
print("="*80)

if __name__ == '__main__':
    pass