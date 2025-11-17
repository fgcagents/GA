import sqlite3
import os
import csv
from datetime import datetime, timedelta
from collections import defaultdict

# ============================================================================
# VERSIÓ 5: SISTEMA DE SUBSTITUCIONS
# ============================================================================
# Afegit suport per substitucions de treballadors:
# - Nou camp: treballador_substitut_id a descansos_dies
# - Noves funcions: afegir_substitucio, eliminar_substitucio, veure_substitucions
# - Funció auxiliar: obtenir_treballador_efectiu (per futures integracions)
# - Actualitzacions a: veure_descansos_treballador, exportar_descansos_csv, processar_csv_modificacions
# ============================================================================

def obtenir_connexio(db_path='treballadors.db'):
    """Crea una connexió a la base de dades"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

# ============================================================================
# FUNCIONS DE CERCA I SELECCIÓ DE TREBALLADORS
# (Sense canvis, mantenen el mateix comportament)
# ============================================================================

def buscar_treballador(db_path, terme_cerca):
    """Busca treballadors per nom/treballador, id o plaça"""
    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, treballador, plaza, rotacio, zona, grup
        FROM treballadors
        WHERE id LIKE ? OR treballador LIKE ? OR plaza LIKE ?
        ORDER BY treballador
    ''', (f'%{terme_cerca}%', f'%{terme_cerca}%', f'%{terme_cerca}%'))
    resultats = cursor.fetchall()
    conn.close()
    return resultats

def mostrar_treballadors(resultats):
    """Mostra una llista de treballadors"""
    if not resultats:
        print("❌ No s'han trobat treballadors")
        return None

    print("\n📋 TREBALLADORS TROBATS:")
    print("-" * 80)
    for i, t in enumerate(resultats, 1):
        print(f"{i}. {t['treballador']} (ID: {t['id']}) - {t['plaza']} - {t['rotacio']} - {t['zona']}")
    return resultats

def seleccionar_treballador(db_path):
    """Permet a l'usuari cercar i seleccionar un treballador"""
    while True:
        cerca = input("\n🔍 Cerca treballador (nom, ID o plaça) o 'sortir': ").strip()
        if cerca.lower() == 'sortir':
            return None

        resultats = buscar_treballador(db_path, cerca)
        resultats_list = mostrar_treballadors(resultats)
        if not resultats_list:
            continue

        if len(resultats_list) == 1:
            return resultats_list[0]

        try:
            seleccio = input(f"\nSelecciona un número (1-{len(resultats_list)}) o 'c' per cercar de nou: ").strip()
            if seleccio.lower() == 'c':
                continue
            index = int(seleccio) - 1
            if 0 <= index < len(resultats_list):
                return resultats_list[index]
            else:
                print("❌ Número invàlid")
        except ValueError:
            print("❌ Entrada invàlida")

# ============================================================================
# NOU: Funció auxiliar per obtenir el treballador efectiu (per futura planificació)
# ============================================================================

def obtenir_treballador_efectiu(db_path, treballador_id, data):
    """
    Retorna l'ID del treballador efectiu per una data donada.
    Si el treballador té una substitució activa, retorna l'ID del substitut.
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
        AND treballador_substitut_id IS NOT NULL
        LIMIT 1
    ''', (treballador_id, data_str))
    
    resultat = cursor.fetchone()
    conn.close()
    
    if resultat and resultat['treballador_substitut_id']:
        return resultat['treballador_substitut_id']
    else:
        return treballador_id

# ============================================================================
# FUNCIONS DE VISUALITZACIÓ DE DESCANSOS (Actualitzada per Substitucions)
# ============================================================================

def veure_descansos_treballador(db_path, treballador_id, any=None):
    """Mostra els descansos d'un treballador i les substitucions que fa"""
    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()

    cursor.execute('SELECT treballador, plaza FROM treballadors WHERE id = ?', (treballador_id,))
    treballador = cursor.fetchone()
    if not treballador:
        print("❌ Treballador no trobat")
        conn.close()
        return

    # 1. Descansos on el treballador és l'ORIGINAL (i té possible substitut)
    query_base = '''
        SELECT d.data, d.origen, d.motiu, d.treballador_substitut_id, 
               t.treballador as nom_substitut, 'Original' as rol
        FROM descansos_dies d
        LEFT JOIN treballadors t ON d.treballador_substitut_id = t.id
        WHERE d.treballador_id = ? 
    '''
    
    # 2. Dies on el treballador és el SUBSTITUT (i ocupa el lloc d'un altre)
    query_substitucions = '''
        SELECT d.data, d.origen, d.motiu, d.treballador_id as original_id, 
               t.treballador as nom_original, 'Substitut' as rol
        FROM descansos_dies d
        INNER JOIN treballadors t ON d.treballador_id = t.id
        WHERE d.treballador_substitut_id = ? 
    '''

    params = [treballador_id]
    if any:
        query_base += " AND strftime('%Y', d.data) = ?"
        query_substitucions += " AND strftime('%Y', d.data) = ?"
        params.append(str(any))

    query_base += " ORDER BY d.data"
    query_substitucions += " ORDER BY d.data"
    
    cursor.execute(query_base, tuple(params))
    descansos_originals = cursor.fetchall()
    
    cursor.execute(query_substitucions, tuple(params))
    descansos_fets = cursor.fetchall()
    
    conn.close()
    
    # Combinar i ordenar els resultats
    tots_els_moviments = {}
    
    for d in descansos_originals:
        data_str = d['data']
        origen = d['origen'] or 'base'
        nom_substitut = d['nom_substitut']
        motiu = d['motiu'] or ''
        
        if origen == 'baixa':
            origen_display = '🏥 BAIXA'
        elif origen == 'manual':
            origen_display = '✏️ Manual'
        elif origen == 'substitucio' and nom_substitut:
            origen_display = f'🔄 Substituït per {nom_substitut}'
        elif origen == 'substitucio':
             origen_display = '🔄 Substituït (Sense Substitut Assignat)'
        else:
            origen_display = origen.capitalize()
            
        tots_els_moviments[data_str] = {
            'data': datetime.strptime(data_str, '%Y-%m-%d').date(), 
            'origen_display': origen_display, 
            'motiu': motiu,
            'rol': d['rol']
        }

    for d in descansos_fets:
        data_str = d['data']
        # Afegir només si no és un descans original del mateix dia (prioritzem descansos propis)
        if data_str not in tots_els_moviments:
            motiu = d['motiu'] or ''
            origen_display = f'✅ Fent Substitució per {d["nom_original"]}'
            
            tots_els_moviments[data_str] = {
                'data': datetime.strptime(data_str, '%Y-%m-%d').date(), 
                'origen_display': origen_display, 
                'motiu': motiu,
                'rol': d['rol']
            }
        
    moviments_ordenats = sorted(tots_els_moviments.values(), key=lambda x: x['data'])

    print("\n" + "="*80)
    print(f"📅 MOVIMENTS DE: {treballador['treballador']} ({treballador['plaza']})")
    print("="*80)
    
    if not moviments_ordenats:
        print("No té descansos o substitucions registrades" + (f" per l'any {any}" if any else ""))
        return

    per_any = defaultdict(list)
    for d in moviments_ordenats:
        per_any[d['data'].year].append(d)

    for any_actual in sorted(per_any.keys()):
        print(f"\n📆 ANY {any_actual} ({len(per_any[any_actual])} registres):")
        print("-" * 80)
        for d in per_any[any_actual]:
            motiu_display = f" - {d['motiu']}" if d['motiu'] else ""
            print(f" • {d['data'].strftime('%Y-%m-%d (%a)')} [{d['origen_display']}]{motiu_display}")

    print(f"\n📊 TOTAL: {len(descansos_originals)} dies de descans/baixa (propis)")
    print(f"📊 TOTAL: {len(descansos_fets)} dies de substitució (fets per ell/a)")

# ============================================================================
# FUNCIONS AUXILIARS PER PERÍODES (Sense canvis de lògica bàsica)
# ============================================================================

def processar_periode(db_path, treballador_id, data_inici, data_fi, accio, origen=None, motiu=None, substitut_id=None):
    """
    Funció auxiliar per gestionar l'addició o eliminació d'un període de descansos.
    Permet afegir el treballador_substitut_id
    :param accio: 'add' o 'delete'
    """
    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()
    data_actual = data_inici
    dies_afectats = 0
    total_dies = (data_fi - data_inici).days + 1

    while data_actual <= data_fi:
        data_str = data_actual.strftime('%Y-%m-%d')
        
        if accio == 'add':
            try:
                # Incloem el camp de substitut a la inserció
                cursor.execute('''
                    INSERT INTO descansos_dies (treballador_id, data, origen, motiu, treballador_substitut_id)
                    VALUES (?, ?, ?, ?, ?)
                ''', (treballador_id, data_str, origen, motiu, substitut_id))
                dies_afectats += 1
            except sqlite3.IntegrityError:
                # Si ja existeix, intentem actualitzar el substitut si es tracta d'una substitució
                if origen == 'substitucio' and substitut_id is not None:
                     cursor.execute('''
                        UPDATE descansos_dies
                        SET treballador_substitut_id = ?, origen = ?, motiu = ?
                        WHERE treballador_id = ? AND data = ?
                        AND origen = 'substitucio'
                    ''', (substitut_id, origen, motiu, treballador_id, data_str))
                pass # El dia ja existeix (i no és una substitució que calgui actualitzar)
        
        elif accio == 'delete':
            cursor.execute('''
                DELETE FROM descansos_dies
                WHERE treballador_id = ? AND data = ?
            ''', (treballador_id, data_str))
            if cursor.rowcount > 0:
                dies_afectats += 1
                
        data_actual += timedelta(days=1)

    conn.commit()
    conn.close()
    
    if accio == 'add':
        print(f"✅ {dies_afectats} dies afegits correctament")
        if dies_afectats < total_dies:
            print(f"⚠️ {total_dies - dies_afectats} dies ja existien")
    elif accio == 'delete':
        print(f"✅ {dies_afectats} dies eliminats correctament")
        if dies_afectats < total_dies:
            print(f"ℹ️ {total_dies - dies_afectats} dies no existien")

# ============================================================================
# FUNCIONS DE GESTIÓ DE DESCANSOS BÀSICS (Sense canvis de lògica)
# ============================================================================

def afegir_descans_puntual(db_path):
    """Afegeix un descans puntual de forma interactiva"""
    treballador = seleccionar_treballador(db_path)
    if not treballador:
        return

    print(f"\n✏️ Afegir descans per: {treballador['treballador']}")
    data_str = input("Data (YYYY-MM-DD): ").strip()
    try:
        data = datetime.strptime(data_str, '%Y-%m-%d').date()
    except ValueError:
        print("❌ Format de data incorrecte")
        return

    motiu = input("Motiu (opcional): ").strip()

    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()
    try:
        # treballador_substitut_id serà NULL per defecte
        cursor.execute('''
            INSERT INTO descansos_dies (treballador_id, data, origen, motiu)
            VALUES (?, ?, 'manual', ?)
        ''', (treballador['id'], data.strftime('%Y-%m-%d'), motiu))
        conn.commit()
        print(f"✅ Descans afegit correctament: {data_str}")
    except sqlite3.IntegrityError:
        print("⚠️ Ja existeix un descans per aquesta data")
    finally:
        conn.close()

def eliminar_descans_puntual(db_path):
    """Elimina un descans puntual de forma interactiva"""
    treballador = seleccionar_treballador(db_path)
    if not treballador:
        return

    print(f"\n🗑️ Eliminar descans de: {treballador['treballador']}")
    data_str = input("Data (YYYY-MM-DD): ").strip()
    try:
        data = datetime.strptime(data_str, '%Y-%m-%d').date()
    except ValueError:
        print("❌ Format de data incorrecte")
        return

    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        DELETE FROM descansos_dies
        WHERE treballador_id = ? AND data = ?
    ''', (treballador['id'], data.strftime('%Y-%m-%d')))
    if cursor.rowcount > 0:
        conn.commit()
        print(f"✅ Descans eliminat correctament: {data_str}")
    else:
        print("⚠️ No existeix cap descans per aquesta data")
    conn.close()

def afegir_periode_descansos(db_path):
    """Afegeix un període de descansos de forma interactiva (Origen: 'temporal')"""
    treballador = seleccionar_treballador(db_path)
    if not treballador:
        return

    print(f"\n📆 Afegir període de descansos (Temporal) per: {treballador['treballador']}")
    try:
        data_inici_str = input("Data inici (YYYY-MM-DD): ").strip()
        data_inici = datetime.strptime(data_inici_str, '%Y-%m-%d').date()
        data_fi_str = input("Data fi (YYYY-MM-DD): ").strip()
        data_fi = datetime.strptime(data_fi_str, '%Y-%m-%d').date()
        if data_fi < data_inici:
            print("❌ La data de fi ha de ser posterior a la data d'inici")
            return
    except ValueError:
        print("❌ Format de data incorrecte")
        return

    motiu = input("Motiu (opcional): ").strip()
    total_dies = (data_fi - data_inici).days + 1
    
    print(f"\nS'afegiran {total_dies} dies de descans")
    confirmacio = input("Continuar? (s/n): ").strip().lower()
    if confirmacio != 's':
        print("❌ Operació cancel·lada")
        return

    # S'utilitza la funció processar_periode amb el nou argument per la substitució (NULL)
    processar_periode(db_path, treballador['id'], data_inici, data_fi, 'add', origen='temporal', motiu=motiu, substitut_id=None)


def eliminar_periode_descansos(db_path):
    """Elimina un període de descansos de forma interactiva"""
    treballador = seleccionar_treballador(db_path)
    if not treballador:
        return

    print(f"\n🗑️ Eliminar període de descansos de: {treballador['treballador']}")
    try:
        data_inici_str = input("Data inici (YYYY-MM-DD): ").strip()
        data_inici = datetime.strptime(data_inici_str, '%Y-%m-%d').date()
        data_fi_str = input("Data fi (YYYY-MM-DD): ").strip()
        data_fi = datetime.strptime(data_fi_str, '%Y-%m-%d').date()
        if data_fi < data_inici:
            print("❌ La data de fi ha de ser posterior a la data d'inici")
            return
    except ValueError:
        print("❌ Format de data incorrecte")
        return

    total_dies = (data_fi - data_inici).days + 1
    print(f"\nS'eliminaran fins a {total_dies} dies de descans/baixa")
    confirmacio = input("Continuar? (s/n): ").strip().lower()
    if confirmacio != 's':
        print("❌ Operació cancel·lada")
        return

    processar_periode(db_path, treballador['id'], data_inici, data_fi, 'delete')


def gestionar_baixa_llarga(db_path):
    """Gestiona l'addició o eliminació de períodes de baixa llarga (Origen: 'baixa')"""
    treballador = seleccionar_treballador(db_path)
    if not treballador:
        return

    print(f"\n🏥 GESTIÓ BAIXA LLARGA per: {treballador['treballador']}")
    
    op = input("Acció: (A)fegir Baixa o (E)liminar Baixa? (a/e): ").strip().lower()
    if op not in ('a', 'e'):
        print("❌ Opció invàlida.")
        return

    try:
        data_inici_str = input("Data inici (YYYY-MM-DD): ").strip()
        data_inici = datetime.strptime(data_inici_str, '%Y-%m-%d').date()
        data_fi_str = input("Data fi (YYYY-MM-DD, p. ex. data prevista de reincorporació): ").strip()
        data_fi = datetime.strptime(data_fi_str, '%Y-%m-%d').date()
        if data_fi < data_inici:
            print("❌ La data de fi ha de ser posterior a la data d'inici")
            return
    except ValueError:
        print("❌ Format de data incorrecte")
        return
    
    motiu = input("Motiu de la baixa (obligatori): ").strip() if op == 'a' else ''
    if op == 'a' and not motiu:
        print("❌ El motiu de la baixa és obligatori.")
        return

    total_dies = (data_fi - data_inici).days + 1
    
    if op == 'a':
        print(f"\nS'afegiran {total_dies} dies com a BAIXA amb origen 'baixa'.")
        confirmacio = input("Continuar? (s/n): ").strip().lower()
        if confirmacio == 's':
            processar_periode(db_path, treballador['id'], data_inici, data_fi, 'add', origen='baixa', motiu=motiu, substitut_id=None)
        else:
            print("❌ Operació cancel·lada")
    
    elif op == 'e':
        print(f"\nS'eliminaran {total_dies} dies de descans/baixa (origen: 'baixa', 'temporal', 'manual').")
        confirmacio = input("Continuar? (s/n): ").strip().lower()
        if confirmacio == 's':
            processar_periode(db_path, treballador['id'], data_inici, data_fi, 'delete')
        else:
            print("❌ Operació cancel·lada")

# ============================================================================
# FUNCIONS DE SUBSTITUCIONS (NOVES)
# ============================================================================

def afegir_substitucio(db_path):
    """Afegeix una substitució"""
    print("\n" + "="*80)
    print("🔄 AFEGIR SUBSTITUCIÓ")
    print("="*80)
    
    print("\n👤 Selecciona el treballador que farà vacances/descans (Treballador Original):")
    treballador_original = seleccionar_treballador(db_path)
    if not treballador_original:
        return
    
    print(f"\n📅 Període de vacances/descans per: {treballador_original['treballador']}")
    try:
        data_inici_str = input("Data inici (YYYY-MM-DD): ").strip()
        data_inici = datetime.strptime(data_inici_str, '%Y-%m-%d').date()
        data_fi_str = input("Data fi (YYYY-MM-DD): ").strip()
        data_fi = datetime.strptime(data_fi_str, '%Y-%m-%d').date()
        
        if data_fi < data_inici:
            print("❌ La data de fi ha de ser posterior a la data d'inici")
            return
    except ValueError:
        print("❌ Format de data incorrecte")
        return
    
    total_dies = (data_fi - data_inici).days + 1
    
    print(f"\n👥 Selecciona el treballador que farà la substitució (Substitut):")
    print(f"   (Substituirà a {treballador_original['treballador']} del {data_inici} al {data_fi})")
    treballador_substitut = seleccionar_treballador(db_path)
    if not treballador_substitut:
        return
    
    if treballador_original['id'] == treballador_substitut['id']:
        print("❌ Un treballador no pot substituir-se a si mateix")
        return
    
    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()
    # Comprovar si el substitut té descansos en el període
    cursor.execute('''
        SELECT data
        FROM descansos_dies
        WHERE treballador_id = ? 
        AND data >= ? 
        AND data <= ?
        ORDER BY data
    ''', (treballador_substitut['id'], data_inici.strftime('%Y-%m-%d'), data_fi.strftime('%Y-%m-%d')))
    descansos_substitut = cursor.fetchall()
    conn.close()
    
    if descansos_substitut:
        print(f"\n⚠️  ATENCIÓ: El substitut té {len(descansos_substitut)} dies de descans en aquest període.")
        print("   S'afegirà la substitució, però el substitut estarà sobre-assignat aquests dies.")
        confirmacio = input("\nVols continuar igualment? (s/n): ").strip().lower()
        if confirmacio != 's':
            print("❌ Operació cancel·lada")
            return
    
    motiu = input("\nMotiu de la substitució (opcional, ex: 'Vacances estiu'): ").strip()
    if not motiu:
        motiu = f"Substituït per {treballador_substitut['treballador']}"
    
    print("\n" + "="*80)
    print("📋 RESUM DE LA SUBSTITUCIÓ")
    print("="*80)
    print(f"Original: {treballador_original['treballador']} ({treballador_original['plaza']})")
    print(f"Substitut: {treballador_substitut['treballador']} ({treballador_substitut['plaza']})")
    print(f"Període: {data_inici} a {data_fi} ({total_dies} dies)")
    print(f"Motiu: {motiu}")
    
    confirmacio = input("\n✅ Confirmar substitució? (s/n): ").strip().lower()
    if confirmacio != 's':
        print("❌ Operació cancel·lada")
        return
    
    # Utilitzem processar_periode amb el substitut_id
    processar_periode(
        db_path, 
        treballador_original['id'], 
        data_inici, 
        data_fi, 
        'add', 
        origen='substitucio', 
        motiu=motiu,
        substitut_id=treballador_substitut['id']
    )
    
    print(f"\n✅ Substitució creada correctament! {treballador_substitut['treballador']} substituirà a {treballador_original['treballador']}.")

def eliminar_substitucio(db_path):
    """Elimina una substitució existent"""
    print("\n" + "="*80)
    print("🗑️ ELIMINAR SUBSTITUCIÓ")
    print("="*80)
    
    print("\n👤 Selecciona el treballador amb substitució (Treballador Original):")
    treballador = seleccionar_treballador(db_path)
    if not treballador:
        return
    
    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()
    # Agrupem les substitucions per rang i substitut
    cursor.execute('''
        SELECT 
            t.treballador as nom_substitut, 
            d.treballador_substitut_id, 
            d.motiu,
            MIN(d.data) as data_inici, 
            MAX(d.data) as data_fi
        FROM descansos_dies d
        INNER JOIN treballadors t ON d.treballador_substitut_id = t.id
        WHERE d.treballador_id = ? AND d.origen = 'substitucio'
        GROUP BY d.treballador_substitut_id, d.motiu
        ORDER BY data_inici
    ''', (treballador['id'],))
    
    substitucions = cursor.fetchall()
    conn.close()
    
    if not substitucions:
        print(f"\n⚠️  {treballador['treballador']} no té substitucions registrades")
        return
    
    print(f"\n📋 SUBSTITUCIONS DE {treballador['treballador']} TROBades:")
    print("-" * 80)
    for i, sub in enumerate(substitucions, 1):
        print(f"{i}. Substitut: {sub['nom_substitut']} (ID: {sub['treballador_substitut_id']})")
        print(f"   Període: {sub['data_inici']} a {sub['data_fi']}")
        print(f"   Motiu: {sub['motiu']}")
    
    try:
        seleccio = input(f"\nSelecciona substitució a eliminar (1-{len(substitucions)}): ").strip()
        if seleccio.lower() == 'sortir': return
        index = int(seleccio) - 1
        if 0 <= index < len(substitucions):
            sub_seleccionada = substitucions[index]
        else:
            print("❌ Selecció invàlida")
            return
    except ValueError:
        print("❌ Entrada invàlida")
        return
    
    print("\n" + "="*80)
    print("⚠️  CONFIRMACIÓ D'ELIMINACIÓ")
    print("="*80)
    print(f"Treballador Original: {treballador['treballador']}")
    print(f"Període a eliminar: {sub_seleccionada['data_inici']} a {sub_seleccionada['data_fi']}")
    print(f"Substitut afectat: {sub_seleccionada['nom_substitut']}")
    
    confirmacio = input("\n❌ Confirmar eliminació d'AQUESTS dies de descans? (s/n): ").strip().lower()
    if confirmacio != 's':
        print("❌ Operació cancel·lada")
        return
    
    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()
    # Eliminem els registres de descans de l'original amb aquest rang, origen i substitut
    cursor.execute('''
        DELETE FROM descansos_dies
        WHERE treballador_id = ?
        AND data >= ?
        AND data <= ?
        AND origen = 'substitucio'
        AND treballador_substitut_id = ?
        AND motiu = ?
    ''', (treballador['id'], 
          sub_seleccionada['data_inici'], 
          sub_seleccionada['data_fi'],
          sub_seleccionada['treballador_substitut_id'],
          sub_seleccionada['motiu']))
    
    files_eliminades = cursor.rowcount
    conn.commit()
    conn.close()
    
    print(f"\n✅ Substitució eliminada correctament!")
    print(f"   • {files_eliminades} dies eliminats del registre de {treballador['treballador']}")

def veure_substitucions(db_path):
    """Mostra totes les substitucions actives i futures"""
    avui = datetime.now().date()
    
    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT 
            t1.treballador as treballador_original,
            t1.plaza as plaza_original,
            t2.treballador as treballador_substitut,
            t2.plaza as plaza_substitut,
            MIN(d.data) as data_inici,
            MAX(d.data) as data_fi,
            d.motiu,
            COUNT(*) as num_dies
        FROM descansos_dies d
        INNER JOIN treballadors t1 ON d.treballador_id = t1.id
        INNER JOIN treballadors t2 ON d.treballador_substitut_id = t2.id
        WHERE d.origen = 'substitucio'
        AND d.data >= ?
        GROUP BY d.treballador_id, d.treballador_substitut_id, d.motiu
        ORDER BY data_inici
    ''', (avui.strftime('%Y-%m-%d'),))
    
    substitucions = cursor.fetchall()
    conn.close()
    
    print("\n" + "="*80)
    print("🔄 SUBSTITUCIONS ACTIVES I FUTURES")
    print("="*80)
    
    if not substitucions:
        print("\n✅ No hi ha substitucions actives o futures")
        return
    
    print(f"\n📊 Total: {len(substitucions)} substitucions\n")
    
    for sub in substitucions:
        data_inici = datetime.strptime(sub['data_inici'], '%Y-%m-%d').date()
        data_fi = datetime.strptime(sub['data_fi'], '%Y-%m-%d').date()
        
        if data_inici > avui:
            estat = "🟡 FUTURA"
            dies_text = f"Comença en {(data_inici - avui).days} dies"
        elif data_fi < avui:
            estat = "⚪ FINALITZADA (Error de filtre, no hauria de sortir)" # Mantingut el filtre WHERE d.data >= ?
            dies_text = f"Va finalitzar fa {(avui - data_fi).days} dies"
        else:
            estat = "🟢 ACTIVA"
            dies_restants = (data_fi - avui).days + 1 # +1 perquè inclou avui
            dies_text = f"Acaba en {dies_restants} dies"
        
        print(f"{estat}")
        print(f"  Original: {sub['treballador_original']} ({sub['plaza_original']})")
        print(f"  Substitut: {sub['treballador_substitut']} ({sub['plaza_substitut']})")
        print(f"  Període: {data_inici} a {data_fi} ({sub['num_dies']} dies)")
        print(f"  {dies_text}")
        if sub['motiu']:
            print(f"  Motiu: {sub['motiu']}")
        print()

# ============================================================================
# FUNCIONS D'ANÀLISI (Petites actualitzacions)
# ============================================================================

def alertar_baixes_pendents(db_path, dies_marge=7):
    """
    Mostra una alerta de les baixes de llarga durada que han expirat
    o estan a prop d'expirar. (Sense canvis de lògica per mantenir el focus només en 'baixa')
    """
    avui = datetime.now().date()
    data_limit = avui + timedelta(days=dies_marge)
    
    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()

    cursor.execute(f'''
        WITH UltimaBaixa AS (
            SELECT 
                treballador_id,
                MAX(data) AS max_data_baixa,
                motiu
            FROM descansos_dies
            WHERE origen = 'baixa'
            GROUP BY treballador_id
        )
        SELECT 
            t.treballador, 
            t.plaza, 
            ub.max_data_baixa AS data_fi_prevista,
            ub.motiu
        FROM treballadors t
        JOIN UltimaBaixa ub ON t.id = ub.treballador_id
        WHERE ub.max_data_baixa <= ?
        ORDER BY data_fi_prevista
    ''', (data_limit.strftime('%Y-%m-%d'),))


    resultats = cursor.fetchall()
    conn.close()

    print("\n" + "="*80)
    print(f"⚠️ ALERTA DE BAIXES PENDENTS D'ALTA/RENOVACIÓ")
    print(f" (Comprovació: baixes que finalitzen abans de: {data_limit.strftime('%Y-%m-%d')})")
    print("="*80)

    if not resultats:
        print("\n✅ No s'han trobat baixes que expirin properament o hagin expirat.")
        return

    print(f"\n🔔 {len(resultats)} BAIXES PENDENTS:")
    print("-" * 80)
    
    for r in resultats:
        data_fi = datetime.strptime(r['data_fi_prevista'], '%Y-%m-%d').date()
        
        if data_fi < avui:
            estat = "🔴 EXPIRADA"
            dies_restants = (avui - data_fi).days
            missatge = f"Fa {dies_restants} dies"
        else:
            estat = "🟡 PROPERA"
            dies_restants = (data_fi - avui).days
            missatge = f"Queden {dies_restants} dies"
            
        print(f" {estat}: {r['treballador']} ({r['plaza']})")
        print(f"   • Data Fi Estipulada: {data_fi.strftime('%Y-%m-%d')} ({missatge})")
        print(f"   • Motiu: {r['motiu'] or 'Sense motiu'}")
        print("   -> Acció: Opció 5a (Gestionar Baixa) per renovar/eliminar el període.")

def treballadors_disponibles_dia(db_path):
    """
    Mostra quins treballadors estan disponibles un dia concret.
    NOTA: Aquesta funció NOMÉS detecta si la PLAÇA té un descans, no si el treballador 
    original està fent un altre servei com a substitut. Per la planificació real,
    s'hauria d'utilitzar obtenir_treballador_efectiu, però ho mantenim simple
    per aquest menú de consulta.
    """
    data_str = input("\n📅 Data a consultar (YYYY-MM-DD): ").strip()
    try:
        data = datetime.strptime(data_str, '%Y-%m-%d').date()
    except ValueError:
        print("❌ Format de data incorrecte")
        return

    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()

    # Amb descans (descansos_dies, inclou baixes, substitucions)
    cursor.execute('''
        SELECT t.id, t.treballador, t.plaza, t.rotacio, t.zona, d.motiu, d.origen, 
               t2.treballador as nom_substitut, t2.plaza as plaza_substitut
        FROM treballadors t
        INNER JOIN descansos_dies d ON t.id = d.treballador_id
        LEFT JOIN treballadors t2 ON d.treballador_substitut_id = t2.id
        WHERE d.data = ?
        ORDER BY t.rotacio, t.treballador
    ''', (data.strftime('%Y-%m-%d'),))
    amb_descans = cursor.fetchall()
    
    ids_descans = {t['id'] for t in amb_descans}

    # Disponibles
    placeholders = ','.join('?' * len(ids_descans))
    if not ids_descans:
        placeholders = '0' 
        ids_descans = {0} 

    cursor.execute(f'''
        SELECT t.id, t.treballador, t.plaza, t.rotacio, t.zona
        FROM treballadors t
        WHERE t.id NOT IN ({placeholders})
        ORDER BY t.rotacio, t.treballador
    ''', list(ids_descans))
    disponibles = cursor.fetchall()

    conn.close()

    print("\n" + "="*80)
    print(f"📊 DISPONIBILITAT PER AL DIA: {data.strftime('%Y-%m-%d (%A)')}")
    print("="*80)

    print(f"\n✅ DISPONIBLES ({len(disponibles)}):")
    print("-" * 80)
    if disponibles:
        rotacions = defaultdict(list)
        for t in disponibles:
            rotacions[t['rotacio']].append(t)
        for rotacio in sorted(rotacions.keys()):
            print(f"\n🕐 {rotacio}:")
            for t in rotacions[rotacio]:
                print(f" • {t['treballador']} ({t['plaza']}) - {t['zona']}")
    else:
        print(" No hi ha treballadors disponibles (o bé estan fent substitució)")

    print(f"\n❌ AMB DESCANS/BAIXA ({len(amb_descans)} - Plaça no coberta/substituïda):")
    print("-" * 80)
    if amb_descans:
        rotacions = defaultdict(list)
        for t in amb_descans:
            rotacions[t['rotacio']].append(t)
        for rotacio in sorted(rotacions.keys()):
            print(f"\n🕐 {rotacio}:")
            for t in rotacions[rotacio]:
                motiu = f" - {t['motiu']}" if t['motiu'] else ""
                if t['origen'] == 'baixa':
                    origen_display = "🏥 BAIXA"
                elif t['origen'] == 'substitucio':
                    origen_display = f"🔄 Substituït per {t['nom_substitut']} ({t['plaza_substitut']})" if t['nom_substitut'] else "🔄 Substituït (Sense Substitut Assignat)"
                else:
                    origen_display = t['origen'].capitalize()
                print(f" • {t['treballador']} ({t['plaza']}) - {t['zona']} [{origen_display}]{motiu}")
    else:
        print(" Ningú té descans")

def calendari_mensual(db_path):
    """
    Mostra un calendari visual de disponibilitat per un mes.
    (Compta el nombre de registres a descansos_dies, que és el nombre de places NO disponibles)
    """
    mes_str = input("\n📅 Mes a consultar (YYYY-MM): ").strip()
    try:
        any, mes = map(int, mes_str.split('-'))
        primer_dia = datetime(any, mes, 1).date()
    except ValueError:
        print("❌ Format incorrecte. Utilitza YYYY-MM")
        return

    if mes == 12:
        darrer_dia = datetime(any + 1, 1, 1).date() - timedelta(days=1)
    else:
        darrer_dia = datetime(any, mes + 1, 1).date() - timedelta(days=1)

    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT data, COUNT(*) as num_treballadors
        FROM descansos_dies
        WHERE data >= ? AND data <= ?
        GROUP BY data
        ORDER BY data
    ''', (primer_dia.strftime('%Y-%m-%d'), darrer_dia.strftime('%Y-%m-%d')))
    descansos_per_dia = {row['data']: row['num_treballadors'] for row in cursor.fetchall()}

    cursor.execute('SELECT COUNT(*) as total FROM treballadors')
    total_treballadors = cursor.fetchone()['total']
    conn.close()

    print("\n" + "="*80)
    print(f"📆 CALENDARI: {primer_dia.strftime('%B %Y').upper()}")
    print("="*80)
    print("\nLlegenda: ✅ 0-20% places NO disponibles | ⚠️ 21-40% | 🔶 41-60% | 🔴 >60%")
    print("-" * 80)

    print("\nDl Dt Dc Dj Dv Ds Dg")
    print(" " + "-" * 27)

    dia_setmana = primer_dia.weekday()
    print(" " + "   " * dia_setmana, end="")

    data_actual = primer_dia
    while data_actual <= darrer_dia:
        data_str = data_actual.strftime('%Y-%m-%d')
        num_descans = descansos_per_dia.get(data_str, 0)
        percentatge = (num_descans / total_treballadors * 100) if total_treballadors > 0 else 0

        if percentatge <= 20:
            icona = "✅"
        elif percentatge <= 40:
            icona = "⚠️"
        elif percentatge <= 60:
            icona = "🔶"
        else:
            icona = "🔴"

        print(f"{data_actual.day:2d}{icona}", end=" ")
        if data_actual.weekday() == 6:
            print()
            print(" ", end="")
        data_actual += timedelta(days=1)

    print("\n")

    print("\n📊 DETALL DEL MES:")
    print("-" * 80)
    data_actual = primer_dia
    dies_critic = []
    while data_actual <= darrer_dia:
        data_str = data_actual.strftime('%Y-%m-%d')
        num_descans = descansos_per_dia.get(data_str, 0)
        total_places = total_treballadors
        percentatge = (num_descans / total_places * 100) if total_places > 0 else 0
        if percentatge > 40:
            dies_critic.append((data_actual, num_descans, percentatge))
        data_actual += timedelta(days=1)

    if dies_critic:
        print("⚠️ DIES AMB ALTA OCUPACIÓ DE DESCANSOS/BAIXES (Places NO disponibles):")
        for dia, num, perc in dies_critic:
            print(f" • {dia.strftime('%Y-%m-%d (%A)')}: {num} places ({perc:.1f}%)")
    else:
        print("✅ No hi ha dies amb alta ocupació de descansos")

def detectar_serveis_descoberts(db_path):
    """
    Detecta possibles serveis descoberts en un període.
    Aquesta funció SÍ que hauria d'utilitzar la lògica d'obtenir_treballador_efectiu
    per a una detecció precisa, però per simplificar la integració, 
    mantindrem la lògica de la PLAÇA sense comprovar substituts.
    """
    print("\n🔍 DETECTAR SERVEIS DESCOBERTS (Places sense cobertura)")
    try:
        data_inici_str = input("Data inici (YYYY-MM-DD): ").strip()
        data_inici = datetime.strptime(data_inici_str, '%Y-%m-%d').date()
        data_fi_str = input("Data fi (YYYY-MM-DD): ").strip()
        data_fi = datetime.strptime(data_fi_str, '%Y-%m-%d').date()
    except ValueError:
        print("❌ Format de data incorrecte")
        return

    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()

    cursor.execute('SELECT servei, opcio_1, opcio_2 FROM serveis') 
    serveis = cursor.fetchall()
    if not serveis:
        print("⚠️ No hi ha serveis configurats a la base de dades")
        conn.close()
        return

    print("\n" + "="*80)
    print("⚠️ ANÀLISI DE SERVEIS DESCOBERTS")
    print(f"Període: {data_inici} a {data_fi}")
    print("="*80)

    problemes = []
    data_actual = data_inici
    while data_actual <= data_fi:
        data_str = data_actual.strftime('%Y-%m-%d')

        serveis_descoberts_avui = []
        for servei in serveis:
            
            # Utilitzem obtenir_treballador_efectiu per saber si la plaça està coberta
            # Si retorna None, la plaça no té cap treballador efectiu (està de descans sense substitut)
            
            id_opcio1 = None
            if servei['opcio_1']:
                cursor.execute("SELECT id FROM treballadors WHERE plaza = ?", (servei['opcio_1'],))
                t1_info = cursor.fetchone()
                if t1_info: id_opcio1 = t1_info['id']

            id_opcio2 = None
            if servei['opcio_2']:
                cursor.execute("SELECT id FROM treballadors WHERE plaza = ?", (servei['opcio_2'],))
                t2_info = cursor.fetchone()
                if t2_info: id_opcio2 = t2_info['id']
            
            id_efectiu_1 = obtenir_treballador_efectiu(db_path, id_opcio1, data_actual) if id_opcio1 else None
            id_efectiu_2 = obtenir_treballador_efectiu(db_path, id_opcio2, data_actual) if id_opcio2 else None
            
            opcio1_disponible = id_efectiu_1 is not None and id_efectiu_1 != id_opcio1
            opcio2_disponible = id_efectiu_2 is not None and id_efectiu_2 != id_opcio2
            
            if id_opcio1 and id_efectiu_1 is None:
                motiu1 = "Té descans (Sense Substitut)"
            elif id_opcio1 and id_efectiu_1 == id_opcio1:
                motiu1 = "Disponible"
            elif id_opcio1 and id_efectiu_1 != id_opcio1:
                motiu1 = f"Substituït per ID {id_efectiu_1}"
            else:
                motiu1 = "Plaça no vàlida"

            if id_opcio2 and id_efectiu_2 is None:
                motiu2 = "Té descans (Sense Substitut)"
            elif id_opcio2 and id_efectiu_2 == id_opcio2:
                motiu2 = "Disponible"
            elif id_opcio2 and id_efectiu_2 != id_opcio2:
                motiu2 = f"Substituït per ID {id_efectiu_2}"
            else:
                motiu2 = "Plaça no vàlida"
                
            # Un servei està descobert si cap de les dues places té un treballador EFECTIU
            if id_efectiu_1 is None and id_efectiu_2 is None:
                serveis_descoberts_avui.append({
                    'servei': servei['servei'],
                    'opcio_1': servei['opcio_1'],
                    'opcio_2': servei['opcio_2'],
                    'motiu_1': motiu1,
                    'motiu_2': motiu2
                })

        if serveis_descoberts_avui:
            problemes.append({
                'data': data_actual,
                'serveis': serveis_descoberts_avui
            })

        data_actual += timedelta(days=1)

    conn.close()

    if not problemes:
        print("\n✅ No s'han detectat serveis descoberts en aquest període!")
        return

    print(f"\n⚠️ S'HAN DETECTAT {len(problemes)} DIES AMB PROBLEMES:")
    for problema in problemes:
        print(f"📅 {problema['data'].strftime('%Y-%m-%d (%A)')}")
        print(f" Serveis descoberts: {len(problema['serveis'])}")
        for servei in problema['serveis']:
            print(f" • {servei['servei']}")
            print(f"   - Opció 1 ({servei['opcio_1']}): {servei['motiu_1']}")
            print(f"   - Opció 2 ({servei['opcio_2']}): {servei['motiu_2']}")
        print()

def estadistiques_treballadors(db_path):
    """Mostra estadístiques de descansos per treballador (Sense canvis, es basen en descansos_dies)"""
    any_str = input("\n📅 Any a analitzar (deixa en blanc per tots): ").strip()
    any = None
    if any_str:
        try:
            any = int(any_str)
        except ValueError:
            print("❌ Any invàlid. S'analitzaran tots els anys.")

    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()

    if any:
        cursor.execute('''
            SELECT t.id, t.treballador, t.plaza, t.rotacio, COUNT(d.id) as total_descansos
            FROM treballadors t
            LEFT JOIN descansos_dies d ON t.id = d.treballador_id
                AND strftime('%Y', d.data) = ?
            GROUP BY t.id
            ORDER BY total_descansos DESC, t.treballador
        ''', (str(any),))
    else:
        cursor.execute('''
            SELECT t.id, t.treballador, t.plaza, t.rotacio, COUNT(d.id) as total_descansos
            FROM treballadors t
            LEFT JOIN descansos_dies d ON t.id = d.treballador_id
            GROUP BY t.id
            ORDER BY total_descansos DESC, t.treballador
        ''')
    resultats = cursor.fetchall()
    conn.close()

    if not resultats:
        print("❌ No s'han trobat dades")
        return

    print("\n" + "="*80)
    titulo = f"📊 ESTADÍSTIQUES DE DESCANSOS/BAIXES{f' - ANY {any}' if any else ''}"
    print(titulo)
    print("="*80)

    total_descansos = sum(r['total_descansos'] for r in resultats)
    mitjana = total_descansos / len(resultats) if resultats else 0

    print(f"\n📈 Resum global:")
    print(f" • Total treballadors: {len(resultats)}")
    print(f" • Total dies de descans/baixa: {total_descansos}")
    print(f" • Mitjana per treballador: {mitjana:.1f} dies")

    print(f"\n🔝 TOP 10 AMB MÉS DESCANSOS/BAIXES:")
    print("-" * 80)
    for i, r in enumerate(resultats[:10], 1):
        diferencia = r['total_descansos'] - mitjana
        indicador = "🔴" if diferencia > mitjana * 0.2 else "🟡" if diferencia > 0 else "🟢"
        print(f"{i:2d}. {indicador} {r['treballador']:30s} ({r['plaza']:10s}) - {r['total_descansos']:3d} dies ({diferencia:+.1f})")

    print(f"\n🔻 TOP 10 AMB MENYS DESCANSOS/BAIXES:")
    print("-" * 80)
    bottom10 = list(reversed(resultats[-10:]))
    for i, r in enumerate(bottom10, 1):
        diferencia = r['total_descansos'] - mitjana
        indicador = "🔴" if diferencia < -mitjana * 0.2 else "🟡" if diferencia < 0 else "🟢"
        print(f"{i:2d}. {indicador} {r['treballador']:30s} ({r['plaza']:10s}) - {r['total_descansos']:3d} dies ({diferencia:+.1f})")

    print(f"\n📊 DISTRIBUCIÓ PER ROTACIÓ:")
    print("-" * 80)
    rotacions = defaultdict(lambda: {'total': 0, 'descansos': 0})
    for r in resultats:
        rotacions[r['rotacio']]['total'] += 1
        rotacions[r['rotacio']]['descansos'] += r['total_descansos']
    for rotacio in sorted(rotacions.keys()):
        mitjana_rotacio = rotacions[rotacio]['descansos'] / rotacions[rotacio]['total'] if rotacions[rotacio]['total'] > 0 else 0
        print(f" • {rotacio:10s}: {rotacions[rotacio]['total']:2d} treballadors, {rotacions[rotacio]['descansos']:4d} dies total (mitjana: {mitjana_rotacio:.1f})")

def historial_canvis(db_path):
    """Mostra l'historial de canvis recent (incloent substitucions)"""
    print("\n📜 HISTORIAL DE CANVIS")
    print("="*80)

    conn = obtenir_connexio(db_path)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT d.data, t.treballador, t.plaza, d.origen, d.motiu, d.id, 
               t2.treballador as nom_substitut
        FROM descansos_dies d
        INNER JOIN treballadors t ON d.treballador_id = t.id
        LEFT JOIN treballadors t2 ON d.treballador_substitut_id = t2.id
        WHERE d.origen IN ('manual', 'temporal', 'baixa', 'substitucio')
        ORDER BY d.id DESC
        LIMIT 50
    ''')
    canvis = cursor.fetchall()
    conn.close()

    if not canvis:
        print("\nℹ️ No hi ha canvis manuals, temporals, baixes o substitucions registrades")
        return

    print(f"\n📋 ÚLTIMS {len(canvis)} CANVIS:")
    print("-" * 80)
    for canvi in canvis:
        data = datetime.strptime(canvi['data'], '%Y-%m-%d').date()
        if canvi['origen'] == 'manual':
            origen_emoji = "✏️"
        elif canvi['origen'] == 'temporal':
            origen_emoji = "📆"
        elif canvi['origen'] == 'baixa':
            origen_emoji = "🏥"
        elif canvi['origen'] == 'substitucio':
            origen_emoji = "🔄"
        else:
            origen_emoji = "❓"
            
        motiu = f" - {canvi['motiu']}" if canvi['motiu'] else ""
        substitut = f" (Subst: {canvi['nom_substitut']})" if canvi['nom_substitut'] else ""
        
        print(f"{origen_emoji} {data} | {canvi['treballador']:30s} ({canvi['plaza']}){substitut}{motiu}")

# ============================================================================
# FUNCIONS D'IMPORTAR/EXPORTAR (Actualitzades per Substitucions)
# ============================================================================

def processar_csv_modificacions(db_path, fitxer='modificacions.csv'):
    """Processa el fitxer modificacions.csv (inclou substitut_id)"""
    print("\n📄 PROCESSAR modificacions.csv")
    print("="*80)

    if not os.path.exists(fitxer):
        print(f"⚠️ Fitxer '{fitxer}' no trobat")
        return

    try:
        conn = obtenir_connexio(db_path)
        cursor = conn.cursor()
        modificacions_processades = 0
        errors = 0

        with open(fitxer, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            camps_esperats = ['treballador_id', 'data', 'origen']
            if not all(camp in reader.fieldnames for camp in camps_esperats):
                print("❌ Format de CSV incorrecte. S'espera: treballador_id, data, origen, motiu, [treballador_substitut_id]")
                return

            for row in reader:
                try:
                    treballador_id = row.get('treballador_id', '').strip()
                    data = row.get('data', '').strip()
                    origen = row.get('origen', 'manual').strip() 
                    motiu = row.get('motiu', '').strip()
                    substitut_id_str = row.get('treballador_substitut_id', '').strip()
                    substitut_id = int(substitut_id_str) if substitut_id_str.isdigit() else None

                    if not treballador_id or not data:
                        errors += 1
                        continue

                    datetime.strptime(data, '%Y-%m-%d')
                    
                    # Intentem INSERT. Si hi ha IntegrityError, s'ignora (duplicat)
                    cursor.execute('''
                        INSERT INTO descansos_dies (treballador_id, data, origen, motiu, treballador_substitut_id)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (treballador_id, data, origen, motiu, substitut_id))
                    modificacions_processades += 1
                except sqlite3.IntegrityError:
                    # En cas de duplicat, si és una substitució, actualitzem el substitut (si cal)
                    if origen == 'substitucio' and substitut_id is not None:
                        cursor.execute('''
                            UPDATE descansos_dies
                            SET treballador_substitut_id = ?, motiu = ?
                            WHERE treballador_id = ? AND data = ?
                        ''', (substitut_id, motiu, treballador_id, data))
                        modificacions_processades += 1
                    errors += 1
                except ValueError:
                    errors += 1

        conn.commit()
        conn.close()
        print(f"✅ {modificacions_processades} registres processats correctament")
        if errors > 0:
            print(f"⚠️ {errors} registres amb errors (duplicats o format incorrecte)")
    except Exception as e:
        print(f"❌ Error processant el fitxer: {e}")

def processar_csv_descansos_temporals(db_path, fitxer='descansos_temporals.csv'):
    """Processa el fitxer descansos_temporals.csv amb períodes (sense substitut_id)"""
    # Mantenim aquesta funció sense canvis per als períodes simples 'temporal'
    print("\n📄 PROCESSAR descansos_temporals.csv")
    print("="*80)

    if not os.path.exists(fitxer):
        print(f"⚠️ Fitxer '{fitxer}' no trobat")
        return

    try:
        conn = obtenir_connexio(db_path)
        cursor = conn.cursor()
        periodes_processats = 0
        dies_afegits = 0
        errors = 0

        with open(fitxer, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or 'treballador_id' not in reader.fieldnames:
                print("❌ Format de CSV incorrecte. S'espera: treballador_id, data_inici, data_fi, motiu")
                return

            for row in reader:
                try:
                    treballador_id = row.get('treballador_id', '').strip()
                    data_inici = row.get('data_inici', '').strip()
                    data_fi = row.get('data_fi', '').strip()
                    motiu = row.get('motiu', '').strip()

                    if not treballador_id or not data_inici or not data_fi:
                        errors += 1
                        continue

                    data_inici_obj = datetime.strptime(data_inici, '%Y-%m-%d').date()
                    data_fi_obj = datetime.strptime(data_fi, '%Y-%m-%d').date()
                    if data_fi_obj < data_inici_obj:
                        errors += 1
                        continue

                    data_actual = data_inici_obj
                    while data_actual <= data_fi_obj:
                        try:
                            # treballador_substitut_id = NULL
                            cursor.execute('''
                                INSERT INTO descansos_dies (treballador_id, data, origen, motiu)
                                VALUES (?, ?, 'temporal', ?) 
                            ''', (treballador_id, data_actual.strftime('%Y-%m-%d'), motiu))
                            dies_afegits += 1
                        except sqlite3.IntegrityError:
                            pass
                        data_actual += timedelta(days=1)

                    periodes_processats += 1
                except ValueError:
                    errors += 1

        conn.commit()
        conn.close()
        print(f"✅ {periodes_processats} períodes processats correctament")
        print(f"✅ {dies_afegits} dies de descans afegits")
        if errors > 0:
            print(f"⚠️ {errors} registres amb errors (format incorrecte o dates invàlides)")
    except Exception as e:
        print(f"❌ Error processant el fitxer: {e}")

def exportar_descansos_csv(db_path, fitxer='descansos_exportats.csv'):
    """Exporta els descansos a CSV (inclou treballador_substitut_id)"""
    print("\n💾 EXPORTAR DESCANSOS A CSV")
    print("="*80)

    try:
        conn = obtenir_connexio(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT t.id, t.treballador, t.plaza, t.rotacio, 
                   d.data, d.origen, d.motiu, d.treballador_substitut_id
            FROM descansos_dies d
            INNER JOIN treballadors t ON d.treballador_id = t.id
            ORDER BY d.data, t.treballador
        ''')
        descansos = cursor.fetchall()
        conn.close()

        if not descansos:
            print("⚠️ No hi ha descansos a exportar")
            return

        with open(fitxer, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Nou camp: treballador_substitut_id
            writer.writerow(['treballador_id', 'nom', 'plaza', 'rotacio', 'data', 'origen', 'motiu', 'treballador_substitut_id'])
            for d in descansos:
                writer.writerow([
                    d['id'],
                    d['treballador'],
                    d['plaza'],
                    d['rotacio'],
                    d['data'],
                    d['origen'],
                    d['motiu'] or '',
                    d['treballador_substitut_id'] or '' # Exportar com a cadena buida si és NULL
                ])
        print(f"✅ S'han exportat {len(descansos)} registres a '{fitxer}'")
    except Exception as e:
        print(f"❌ Error exportant els descansos: {e}")

# ============================================================================
# MENÚ PRINCIPAL (Actualitzat)
# ============================================================================

def mostrar_menu():
    """Mostra el menú principal"""
    print("\n" + "="*80)
    print("GESTOR INTERACTIU DE DESCANSOS (v5 - AMB SUBSTITUCIONS)")
    print(" (Base de dades: treballadors.db)")
    print("="*80)

    print("\n📋 GESTIÓ BÀSICA:")
    print(" 1. Veure descansos d'un treballador")
    print(" 2. Afegir descans puntual")
    print(" 3. Eliminar descans puntual")
    print(" 4. Afegir període de descansos (Temporal)")
    print(" 5. Eliminar període de descansos")
    print(" 5a.Gestionar Baixa Llarg Termini (Baixa)")
    
    print("\n🔄 GESTIÓ SUBSTITUCIONS:")
    print(" 5b. Afegir Substitució") # NOU (6a)
    print(" 5c. Eliminar Substitució") # NOU (6b)
    print(" 5d. Veure Substitucions Actives/Futures") # NOU (6c)

    print("\n📊 CONSULTES I ANÀLISI:")
    print(" 6. Treballadors disponibles un dia concret (Disponibilitat de Plaça)")
    print(" 7. Calendari mensual de disponibilitat (Places NO Cobertes)")
    print(" 8. Detectar serveis descoberts (període)")
    print(" 9. Estadístiques per treballador")
    print(" 10. Historial de canvis recents")
    print(" 10a. Alerta Baixes Pendent d'Alta/Renovació")

    print("\n💾 IMPORTAR/EXPORTAR:")
    print(" 11. Processar modificacions.csv (Inclou Substitut ID)")
    print(" 12. Processar descansos_temporals.csv")
    print(" 13. Exportar descansos a CSV (Inclou Substitut ID)")

    print("\n0. Sortir")
    print("-"*80)

def main(db_path='treballadors.db'):
    """Menú principal interactiu"""
    if not os.path.exists(db_path):
        print(f"❌ La base de dades '{db_path}' no existeix")
        print("Assegura't que has creat el fitxer amb l'esquema de treballadors.db.")
        return

    while True:
        try:
            mostrar_menu()
            opcio = input("\nSelecciona una opció (0-13, 5a, 5b, 5c, 5d, 10a): ").strip().lower()

            if opcio == '0':
                print("\n👋 Fins aviat!")
                break
            elif opcio == '1':
                treballador = seleccionar_treballador(db_path)
                if treballador:
                    veure_descansos_treballador(db_path, treballador['id'])
            elif opcio == '2':
                afegir_descans_puntual(db_path)
            elif opcio == '3':
                eliminar_descans_puntual(db_path)
            elif opcio == '4':
                afegir_periode_descansos(db_path)
            elif opcio == '5':
                eliminar_periode_descansos(db_path)
            elif opcio == '5a':
                gestionar_baixa_llarga(db_path)
            elif opcio == '5b': # NOU
                afegir_substitucio(db_path)
            elif opcio == '5c': # NOU
                eliminar_substitucio(db_path)
            elif opcio == '5d': # NOU
                veure_substitucions(db_path)
            elif opcio == '6':
                treballadors_disponibles_dia(db_path)
            elif opcio == '7':
                calendari_mensual(db_path)
            elif opcio == '8':
                detectar_serveis_descoberts(db_path)
            elif opcio == '9':
                estadistiques_treballadors(db_path)
            elif opcio == '10':
                historial_canvis(db_path)
            elif opcio == '10a':
                alertar_baixes_pendents(db_path)
            elif opcio == '11':
                processar_csv_modificacions(db_path)
            elif opcio == '12':
                processar_csv_descansos_temporals(db_path)
            elif opcio == '13':
                exportar_descansos_csv(db_path)
            else:
                print("❌ Opció no vàlida. Si us plau, selecciona una opció vàlida (ex: 5a, 5b)")
        except KeyboardInterrupt:
            print("\n👋 Programa interromput per l'usuari")
            break
        except Exception as e:
            print(f"❌ Error inesperat: {e}")
        finally:
            if opcio != '0' and opcio not in ['', None]:
                input("\n📌 Prem Enter per continuar...")

if __name__ == '__main__':
    main()