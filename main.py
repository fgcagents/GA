# main.py - ACTUALITZAT PER A CÀRREGA DES DE SQLITE

from data_loader import DataLoader
from constraints import (
    RestriccionManager,
    # ... (imports de restriccions)
    restriccio_grup_T,
    restriccio_sense_descans,
    restriccio_formacio_requerida,
    restriccio_linia_correcta,
    restriccio_hores_anuals,
    restriccio_dies_consecutius,
    restriccio_equitat_canvis_zona,
    restriccio_equitat_canvis_torn,
    restriccio_cobertura_completa,
    restriccio_distribucio_equilibrada,
    restriccio_sense_solapaments_rigida,
    restriccio_descans_minim_12h_rigida,
    restriccio_divendres_cap_setmana_rigida,
    restriccio_unica_assignacio_per_dia_rigida

)
from data_structures import EstadistiquesGlobals
from genetic_algorithm import AlgorismeGenetic
import json
import csv
from datetime import datetime, date
from typing import Dict, Set, Optional
import argparse
from collections import Counter

class CustomJSONEncoder(json.JSONEncoder):
    """Encoder personalitzat per serialitzar Sets, dates i times"""
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)  # Converteix Set a llista
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if isinstance(obj, time):
            return obj.strftime('%H:%M')
        return super().default(obj)

def main(start_date: Optional[date] = None, end_date: Optional[date] = None, on_duplicate: Optional[str] = None):
    print("="*70)
    print(" SISTEMA D'ASSIGNACIÓ DE TREBALLADORS - ALGORISME GENÈTIC")
    print("="*70)
    
    # [MODIFICACIÓ] Inicialització del DataLoader i Connexió a SQLite
    data_loader = DataLoader() # Utilitza 'treballadors.db' per defecte
    if not data_loader.connect():
        print("✗ No s'ha pogut establir la connexió a la base de dades.")
        return
    
     # ==================== REINICIAR TAULA ASSIGNACIONS ====================
    print("\n🔄 Reiniciant taula d'assignacions...")
    print("-" * 70)
    data_loader.reinicia_taula_assig_grup_T()
    
    # ==================== 1. CÀRREGA DE DADES ====================
    print("\n📂 FASE 1: Carregant dades...")
    print("-" * 70)
    
    try:
        # [MODIFICACIÓ] Crida a la funció de càrrega sense paràmetre de fitxer
        torns = data_loader.carrega_torns()
        print(f"✓ Torns carregats: {len(torns)}")
    except Exception as e:
        print(f"✗ Error carregant torns: {e}")
        data_loader.close() # Tancar connexió en cas d'error
        return
    
    try:
        # [MODIFICACIÓ] Crida a la funció de càrrega sense paràmetre de fitxer
        calendari = data_loader.carrega_calendari()
        print(f"✓ Dies del calendari: {len(calendari)}")
    except Exception as e:
        print(f"✗ Error carregant calendari: {e}")
        data_loader.close() # Tancar connexió en cas d'error
        return
    
    try:
        # [MODIFICACIÓ] Crida a la funció de càrrega sense paràmetre de fitxer
        treballadors = data_loader.carrega_treballadors()
        print(f"✓ Treballadors disponibles: {len(treballadors)}")
        
        # Mostrem resum per grups
        grups = Counter(t.grup for t in treballadors.values())
        for grup, count in sorted(grups.items()):
            print(f"   - Grup {grup}: {count} treballadors")
        
        treballadors_grup_t = {tid: t for tid, t in treballadors.items() if t.grup == 'T'}
        print(f"   → Treballadors grup T (assignables): {len(treballadors_grup_t)}")
        
    except Exception as e:
        print(f"✗ Error carregant treballadors: {e}")
        data_loader.close() # Tancar connexió en cas d'error
        return

    # =====================================================
    # Càrrega de l'històric d'assignacions (si existeix)
    # =====================================================
    try:
        # [MODIFICACIÓ] Crida a la funció de càrrega sense paràmetre de fitxer
        estadistiques = data_loader.carrega_historic(treballadors)
    except Exception as e:
        print(f"   ℹ️  Creant nou històric (error: {e})")
        estadistiques = EstadistiquesGlobals()
   
    try:
        # [MODIFICACIÓ] Crida a la funció de càrrega sense paràmetre de fitxer
        necessitats = data_loader.carrega_necessitats_cobertura()
        print(f"✓ Necessitats de cobertura: {len(necessitats)}")
    except Exception as e:
        print(f"✗ Error carregant necessitats: {e}")
        data_loader.close() # Tancar connexió en cas d'error
        return
    
    if not necessitats:
        print("\n⚠️  No hi ha necessitats de cobertura per assignar!")
        data_loader.close() # Tancar connexió
        return
    
    # Permetre filtrar per interval indicat per l'usuari
    if start_date or end_date:
        # Determinem límits efectius segons les necessitats existents
        dates_necessitats_all = sorted({n.data for n in necessitats})
        if not dates_necessitats_all:
            print("\n⚠️  No hi ha necessitats de cobertura per assignar!")
            data_loader.close() # Tancar connexió
            return

        default_start = dates_necessitats_all[0]
        default_end = dates_necessitats_all[-1]

        s = start_date or default_start
        e = end_date or default_end

        if s > e:
            # intercanviem per comoditat
            s, e = e, s

        # Fem el filtrat
        necessitats = [n for n in necessitats if s <= n.data <= e]
        calendari = {d: v for d, v in calendari.items() if s <= d <= e}

        if not necessitats:
            print(f"\n⚠️  No hi ha necessitats dins l'interval {s} a {e}.")
            data_loader.close() # Tancar connexió
            return

        dates_necessitats = set(n.data for n in necessitats)
        print(f"\n   Dates a cobrir (filtrat): {min(dates_necessitats)} a {max(dates_necessitats)}")
        print(f"   Total dies diferents (filtrat): {len(dates_necessitats)}")
    else:
        # Mostrem resum de dates
        dates_necessitats = set(n.data for n in necessitats)
        print(f"\n   Dates a cobrir: {min(dates_necessitats)} a {max(dates_necessitats)}")
        print(f"   Total dies diferents: {len(dates_necessitats)}")
    
    
    # ===== Detectar solapaments entre històric i les dates actuals =====
    historic_dates = set()
    for hist in estadistiques.historials.values():
        for a in hist.assignacions_any:
            historic_dates.add(a.data)

    dates_a_cobrir = set(n.data for n in necessitats)
    dates_solapades = sorted(dates_a_cobrir.intersection(historic_dates))

    # Map de exclusió per data -> set(treballador_id) (ús per 'add_new_only')
    exclude_map: Dict[date, Set[str]] = {}

    if dates_solapades:
        print(f"\n\u26a0\ufe0f  Avis: ja existeixen assignacions a l'històric per les dates: {', '.join(str(d) for d in dates_solapades)}")

        # Determinem l'acció a prendre: prioritzem el valor rebut per paràmetre on_duplicate
        choice = on_duplicate

        def ask_on_duplicate():
            print('\nTria una de les opcions per gestionar les assignacions ja existents:')
            print('  1) Actualitzar totes les dades i ELIMINAR les assignacions anteriors per aquestes dates (replace_all)')
            print('  2) Buscar noves incorporacions i AFEGIR-LES, però NO considerar treballadors que ja tenien una assignació per aquestes mateixes dates (add_new_only)')
            print('  3) Cancel·lar (exit)')
            while True:
                resp = input('Introdueix 1, 2 o 3: ').strip()
                if resp == '1':
                    return 'replace_all'
                if resp == '2':
                    return 'add_new_only'
                if resp == '3':
                    print('Cancel·lat el procés per solapament amb històric')
                    data_loader.close() # Tancar connexió abans de sortir
                    exit(0)
                print('Opció no vàlida. Torna-ho a provar.')

        if not choice:
            choice = ask_on_duplicate()

        if choice == 'replace_all':
            # Eliminem les assignacions de l'històric per aquestes dates i ajustem comptadors
            removed_count = 0
            for treb_id, treb in treballadors.items():
                historic = estadistiques.get_historic(treb_id)
                to_keep = []
                for a in historic.assignacions_any:
                    if a.data in dates_solapades:
                        treb.hores_anuals_realitzades = max(0.0, treb.hores_anuals_realitzades - a.durada_hores)
                        if a.es_canvi_zona:
                            treb.canvis_zona = max(0, treb.canvis_zona - 1)
                        if a.es_canvi_torn:
                            treb.canvis_torn = max(0, treb.canvis_torn - 1)
                        removed_count += 1
                    else:
                        to_keep.append(a)
                historic.assignacions_any = to_keep
                historic.ultima_assignacio = to_keep[-1] if to_keep else None

            print(f"   \u2713 S'han eliminat {removed_count} assignacions de l'històric per les dates solapades.")

        elif choice == 'add_new_only':
            # Construir exclude_map: per cada data solapada, recollim els IDs de treballadors amb assignacions
            for hist in estadistiques.historials.values():
                for a in hist.assignacions_any:
                    if a.data in dates_solapades:
                        exclude_map.setdefault(a.data, set()).add(a.treballador_id)

            total_excluded = sum(len(s) for s in exclude_map.values())
            print(f"   \u2713 S'han detectat {len(dates_solapades)} data(s) amb {total_excluded} treballador(s) a excloure per a noves assignacions.")

        else:
            print('   ℹ️ Opció d\'on_duplicate desconeguda; no s\'aplicarà cap exclusió.')
    
    # ==================== 2. CONFIGURACIÓ DE RESTRICCIONS ====================
    print("\n⚙️  FASE 2: Configurant restriccions...")
    print("-" * 70)
    
    restriccions = RestriccionManager()
    
    # ===== RESTRICCIONS CRÍTIQUES (pes alt) =====
    print("\n   🔴 RESTRICCIONS CRÍTIQUES:")

    restriccions.afegeix_restriccio(
        restriccio_unica_assignacio_per_dia_rigida, 
        pes=0.30, 
        nom="🔒 Una assignació per dia (RÍGIDA)"
    )
    print("      • Una assignació per dia (pes: 0.30))")
    
    restriccions.afegeix_restriccio(
        restriccio_grup_T, 
        pes=0.20, 
        nom="👥 Només grup T"
    )
    print("      • Només grup T (pes: 0.20)")
    
    restriccions.afegeix_restriccio(
        restriccio_sense_descans, 
        pes=0.20, 
        nom="❌ Sense descans"
    )
    print("      • Sense descans (pes: 0.20)")
    
    restriccions.afegeix_restriccio(
        restriccio_formacio_requerida, 
        pes=0.15, 
        nom="🎓 Formació requerida"
    )
    print("      • Formació requerida (pes: 0.15)")
    
    restriccions.afegeix_restriccio(
        restriccio_linia_correcta, 
        pes=0.10, 
        nom="🚇 Línia correcta"
    )
    print("      • Línia correcta (pes: 0.10)")
    
    restriccions.afegeix_restriccio(
        restriccio_hores_anuals, 
        pes=0.15, 
        nom="⏰ Hores anuals (màx 1.605h)"
    )
    print("      • Hores anuals màximes (pes: 0.15)")
    
    restriccions.afegeix_restriccio(
        restriccio_cobertura_completa, 
        pes=0.10, 
        nom="📋 Cobertura completa"
    )
    print("      • Cobertura completa (pes: 0.10)")
    
    # ===== RESTRICCIONS RÍGIDES (pes mitjà) =====
    print("\n   🟡 RESTRICCIONS IMPORTANTS:")
    
    restriccions.afegeix_restriccio(
        restriccio_dies_consecutius, 
        pes=0.05, 
        nom="📅 Màx 9 dies consecutius"
    )
    print("      • Màxim 9 dies consecutius (pes: 0.05)")
    
    restriccions.afegeix_restriccio(
        restriccio_descans_minim_12h_rigida, 
        pes=0.25, 
        nom="💤 Descans mínim 12h"
    )
    print("      • Descans mínim 12h entre torns (pes: 0.25)")
    
    restriccions.afegeix_restriccio(
        restriccio_divendres_cap_setmana_rigida, 
        pes=0.15, 
        nom="🏖️  Divendres pre-cap setmana"
    )
    print("      • Divendres acabar abans 22h si descans cap setmana (pes: 0.15)")
    
    restriccions.afegeix_restriccio(
        restriccio_sense_solapaments_rigida, 
        pes=0.25, 
        nom="🕐 Sense solapaments"
    )
    print("      • Sense solapaments (pes: 0.25)")
    
    # ===== RESTRICCIONS D'EQUITAT (bonus) =====
    print("\n   🟢 RESTRICCIONS D'EQUITAT (BONUS):")
    
    restriccions.afegeix_restriccio(
        restriccio_equitat_canvis_zona, 
        pes=0.03, 
        nom="🗺️  Equitat canvis zona"
    )
    print("      • Equitat en canvis de zona (pes: 0.03)")
    
    restriccions.afegeix_restriccio(
        restriccio_equitat_canvis_torn, 
        pes=0.03, 
        nom="🔄 Equitat canvis torn"
    )
    print("      • Equitat en canvis de torn (pes: 0.03)")
    
    restriccions.afegeix_restriccio(
        restriccio_distribucio_equilibrada, 
        pes=0.02, 
        nom="⚖️  Distribució equilibrada"
    )
    print("      • Distribució equilibrada (pes: 0.02)")
    
    print(f"\n   ✓ Total restriccions configurades: {len(restriccions.restriccions)}")
    print(f"   ✓ Suma de pesos: {sum(r['pes'] for r in restriccions.restriccions):.2f}")
    
    # ==================== 3. EXECUCIÓ DE L'ALGORISME GENÈTIC ====================
    print("\n🧬 FASE 3: Executant algorisme genètic...")
    print("-" * 70)
    
    # Paràmetres de l'algorisme
    MIDA_POBLACIO = 50
    GENERACIONS = 150
    
    print(f"   Mida població: {MIDA_POBLACIO}")
    print(f"   Generacions: {GENERACIONS}")
    print()
    
    ag = AlgorismeGenetic(
        treballadors=treballadors,
        torns=torns,
        necessitats=necessitats,
        calendari=calendari,
        restriccions=restriccions,
        estadistiques=estadistiques,
        mida_poblacio=MIDA_POBLACIO,
        exclude_map=exclude_map
    )
    
    millor_solucio, resultat_avaluacio = ag.executa(
        generacions=GENERACIONS,
        verbose=True
    )
    
    # ==================== 4. ACTUALITZAR HISTÒRIC ====================
    print("\n📊 FASE 4: Actualitzant històric...")
    print("-" * 70)
    
    # Afegim les noves assignacions a l'històric
    for assignacio in millor_solucio:
        historic = estadistiques.get_historic(assignacio.treballador_id)
        historic.afegir_assignacio(assignacio)
        
        # Actualitzem els comptadors del treballador
        treb = treballadors[assignacio.treballador_id]
        treb.hores_anuals_realitzades += assignacio.durada_hores
        if assignacio.es_canvi_zona:
            treb.canvis_zona += 1
        if assignacio.es_canvi_torn:
            treb.canvis_torn += 1
    
    print(f"   ✓ Històric actualitzat amb {len(millor_solucio)} noves assignacions")
    
    # ==================== 5. RESULTATS ====================
    print("\n" + "="*70)
    print(" 🎯 MILLOR SOLUCIÓ TROBADA")
    print("="*70)
    
    print(f"\n📊 SCORE TOTAL: {resultat_avaluacio['total']:.2f}/100")
    print(f"📋 Total assignacions: {len(millor_solucio)}")
    print(f"📅 Necessitats cobertes: {len(millor_solucio)}/{len(necessitats)}")
    
    # Detall de scores per restricció
    print("\n📈 Detall per restricció:")
    print("-" * 70)
    
    # Agrupem per tipus
    critiques = []
    importants = []
    equitat = []
    
    for nom, info in resultat_avaluacio['detall'].items():
        if 'error' in info:
            print(f"   {nom}: ❌ ERROR - {info['error']}")
        else:
            entry = (nom, info)
            if info['pes'] >= 0.10:
                critiques.append(entry)
            elif info['pes'] >= 0.03:
                importants.append(entry)
            else:
                equitat.append(entry)
    
    if critiques:
        print("\n   🔴 CRÍTIQUES:")
        for nom, info in critiques:
            barra = "█" * int(info['score'] / 5)
            espais = " " * (20 - len(barra))
            print(f"      {nom}")
            print(f"         Score: {info['score']:5.1f}/100 [{barra}{espais}]")
            print(f"         Contribució: {info['ponderat']:5.2f}")
    
    if importants:
        print("\n   🟡 IMPORTANTS:")
        for nom, info in importants:
            barra = "█" * int(info['score'] / 5)
            espais = " " * (20 - len(barra))
            print(f"      {nom}")
            print(f"         Score: {info['score']:5.1f}/100 [{barra}{espais}]")
            print(f"         Contribució: {info['ponderat']:5.2f}")
    
    if equitat:
        print("\n   🟢 EQUITAT:")
        for nom, info in equitat:
            barra = "█" * int(info['score'] / 5)
            espais = " " * (20 - len(barra))
            print(f"      {nom}")
            print(f"         Score: {info['score']:5.1f}/100 [{barra}{espais}]")
            print(f"         Contribució: {info['ponderat']:5.2f}")
    
    # Estadístiques de treballadors
    print("\n👥 Estadístiques de treballadors:")
    print("-" * 70)
    
    assignacions_per_treb = Counter(a.treballador_id for a in millor_solucio)
    hores_per_treb = {}
    canvis_zona_per_treb = Counter()
    canvis_torn_per_treb = Counter()
    
    for assign in millor_solucio:
        hores_per_treb[assign.treballador_id] = \
            hores_per_treb.get(assign.treballador_id, 0) + assign.durada_hores
        if assign.es_canvi_zona:
            canvis_zona_per_treb[assign.treballador_id] += 1
        if assign.es_canvi_torn:
            canvis_torn_per_treb[assign.treballador_id] += 1
    
    if assignacions_per_treb:
        print(f"   Treballadors utilitzats: {len(assignacions_per_treb)}")
        print(f"   Màxim assignacions/treballador: {max(assignacions_per_treb.values())}")
        print(f"   Mínim assignacions/treballador: {min(assignacions_per_treb.values())}")
        print(f"   Mitjana assignacions/treballador: {sum(assignacions_per_treb.values())/len(assignacions_per_treb):.1f}")
        
        if hores_per_treb:
            total_hores = sum(hores_per_treb.values())
            mitjana_hores = total_hores / len(hores_per_treb)
            print(f"\n   Total hores assignades: {total_hores:.1f}h")
            print(f"   Mitjana hores/treballador: {mitjana_hores:.1f}h")
        
        print("\n   Top 5 treballadors més utilitzats:")
        for treb_id, count in assignacions_per_treb.most_common(5):
            treb = treballadors[treb_id]
            hores = hores_per_treb.get(treb_id, 0)
            hores_totals = treb.hores_anuals_realitzades
            dins_limit = "✓" if hores_totals <= treb.max_hores_anuals else "⚠️"
            canvis_z = canvis_zona_per_treb.get(treb_id, 0)
            canvis_t = canvis_torn_per_treb.get(treb_id, 0)
            
            print(f"      {treb.nom}:")
            print(f"         Assignacions: {count}")
            print(f"         Hores: {hores:.1f}h (Total any: {hores_totals:.1f}h {dins_limit})")
            print(f"         Canvis zona: {canvis_z} | Canvis torn: {canvis_t}")
    
    # Estadístiques globals d'equitat
    print("\n🗺️  Estadístiques d'equitat:")
    print("-" * 70)
    
    mitjana_canvis_zona = estadistiques.mitjana_canvis_zona()
    desviacio_zona = estadistiques.desviacio_canvis_zona()
    mitjana_canvis_torn = estadistiques.mitjana_canvis_torn()
    desviacio_torn = estadistiques.desviacio_canvis_torn()
    
    print(f"   Canvis de zona:")
    print(f"      Mitjana: {mitjana_canvis_zona:.2f}")
    print(f"      Desviació estàndard: {desviacio_zona:.2f}")
    
    print(f"\n   Canvis de torn:")
    print(f"      Mitjana: {mitjana_canvis_torn:.2f}")
    print(f"      Desviació estàndard: {desviacio_torn:.2f}")
    
# ==================== 6. EXPORTACIÓ DE RESULTATS ====================
    print("\n💾 FASE 5: Exportant resultats...")
    print("-" * 70)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # ===== Exportem assignacions a JSON =====
    resultat_export = []
    for assign in millor_solucio:
        treballador = treballadors[assign.treballador_id]
        
        # Busquem la necessitat corresponent
        necessitat = None
        for nec in necessitats:
            if nec.servei == assign.torn_id and nec.data == assign.data:
                necessitat = nec
                break
        
        entry = {
            'data': assign.data.strftime('%Y-%m-%d'),
            'dia_setmana': calendari[assign.data].dia_setmana if assign.data in calendari else '',
            'torn': assign.torn_id,
            'treballador_id': assign.treballador_id,
            'treballador_nom': treballador.nom,
            'treballador_plaza': treballador.plaza,
            'treballador_grup': treballador.grup,
            'hora_inici': assign.hora_inici.strftime('%H:%M'),
            'hora_fi': assign.hora_fi.strftime('%H:%M'),
            'durada_hores': f"{assign.durada_hores:.2f}",
            'linia': necessitat.linia if necessitat else '',
            'zona': necessitat.zona if necessitat else '',
            'formacio': list(necessitat.formacio) if necessitat and necessitat.formacio else [],
            'es_canvi_zona': assign.es_canvi_zona,
            'es_canvi_torn': assign.es_canvi_torn,
            'hores_totals_any': f"{treballador.hores_anuals_realitzades:.2f}"
        }
        
        resultat_export.append(entry)
    
    # Ordenem per data i torn
    resultat_export.sort(key=lambda x: (x['data'], x['torn']))
    
    # Guardem JSON
    fitxer_json = f'assignacions_{timestamp}.json'
    
    with open(fitxer_json, 'w', encoding='utf-8') as f:
        json.dump({
            'metadata': {
                'timestamp': timestamp,
                'score_total': resultat_avaluacio['total'],
                'total_assignacions': len(millor_solucio),
                'total_necessitats': len(necessitats),
                'cobertura_percentatge': (len(millor_solucio) / len(necessitats) * 100) if necessitats else 0,
                'treballadors_utilitzats': len(assignacions_per_treb),
                'total_hores_assignades': sum(hores_per_treb.values()) if hores_per_treb else 0
            },
            'scores_restriccions': {
                nom: {
                    'score': info['score'],
                    'pes': info['pes'],
                    'contribucio': info['ponderat']
                }
                for nom, info in resultat_avaluacio['detall'].items()
                if 'error' not in info
            },
            'estadistiques_equitat': {
                'canvis_zona': {
                    'mitjana': mitjana_canvis_zona,
                    'desviacio': desviacio_zona
                },
                'canvis_torn': {
                    'mitjana': mitjana_canvis_torn,
                    'desviacio': desviacio_torn
                }
            },
            'assignacions': resultat_export
        }, f, indent=2, ensure_ascii=False, cls=CustomJSONEncoder)
    
    print(f"✓ Fitxer JSON creat: {fitxer_json}")
    
    # ===== Guardem CSV per Excel =====
    fitxer_csv = f'assignacions_{timestamp}.csv'
    
    if resultat_export:
        with open(fitxer_csv, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=resultat_export[0].keys())
            writer.writeheader()
            writer.writerows(resultat_export)
        
        print(f"✓ Fitxer CSV creat: {fitxer_csv}")
    
    # ===== Informe d'estadístiques per treballador =====
    fitxer_stats = f'estadistiques_treballadors_{timestamp}.csv'
    
    with open(fitxer_stats, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'ID', 'Nom', 'Grup', 'Plaza', 'Zona', 'Torn', 
            'Assignacions_Periode', 'Hores_Periode', 
            'Hores_Totals_Any', 'Hores_Disponibles',
            'Dins_Limit_Estandard', 'Canvis_Zona_Total', 'Canvis_Torn_Total'
        ])
        
        for treb_id in sorted(assignacions_per_treb.keys(), 
                             key=lambda x: assignacions_per_treb[x], reverse=True):
            treb = treballadors[treb_id]
            historic = estadistiques.get_historic(treb_id)
            
            writer.writerow([
                treb.id,
                treb.nom,
                treb.grup,
                treb.plaza,
                treb.zona,
                treb.torn_assignat,
                assignacions_per_treb[treb_id],
                f"{hores_per_treb.get(treb_id, 0):.2f}",
                f"{treb.hores_anuals_realitzades:.2f}",
                f"{treb.hores_disponibles():.2f}",
                "Sí" if treb.esta_dins_limit_estandard() else "No",
                historic.total_canvis_zona(),
                historic.total_canvis_torn()
            ])
    
    print(f"✓ Estadístiques treballadors: {fitxer_stats}")
    
    # ==================== 7. NECESSITATS NO COBERTES ====================
    assignacions_set = set((a.torn_id, a.data) for a in millor_solucio)
    no_cobertes = [n for n in necessitats if (n.servei, n.data) not in assignacions_set]
    
    if no_cobertes:
        print(f"\n⚠️  Necessitats NO cobertes: {len(no_cobertes)}")
        print("-" * 70)
        
        fitxer_no_cobertes = f'no_cobertes_{timestamp}.csv'
        with open(fitxer_no_cobertes, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Data', 'Torn', 'Formació', 'Línia', 'Zona', 
                           'Torn_Tipus', 'Motiu_Original'])
            
            for nec in no_cobertes[:10]:
                print(f"   • {nec.data} - {nec.servei} ({nec.formacio}, "
                      f"{nec.linia}-{nec.zona}, {nec.torn})")
                writer.writerow([
                    nec.data, nec.servei, nec.formacio, nec.linia, 
                    nec.zona, nec.torn, nec.motiu
                ])
            
            if len(no_cobertes) > 10:
                print(f"   ... i {len(no_cobertes) - 10} més")
            
            for nec in no_cobertes[10:]:
                writer.writerow([
                    nec.data, nec.servei, nec.formacio, nec.linia, 
                    nec.zona, nec.torn, nec.motiu
                ])
        
        print(f"\n✓ Llista completa guardada a: {fitxer_no_cobertes}")
        
        # Analitzem per què no s'han pogut cobrir
        print("\n   Anàlisi de causes possibles:")
        grup_t_disponible = len(treballadors_grup_t)
        print(f"      • Treballadors grup T disponibles: {grup_t_disponible}")
        
        # Comptem quants torns diferents hi ha
        torns_diferents = len(set(n.servei for n in no_cobertes))
        print(f"      • Torns diferents no coberts: {torns_diferents}")
        
        # Comptem per formació
        formacions_str = [', '.join(sorted(n.formacio)) if isinstance(n.formacio, set) else str(n.formacio) 
                          for n in no_cobertes]
        formacions = Counter(formacions_str)
        print(f"      • Formacions més difícils de cobrir:")
        for formacio, count in formacions.most_common(3):
            print(f"         - {formacio}: {count} necessitats")
        
    else:
        print("\n✅ Totes les necessitats han estat cobertes!")
    
    # ==================== 8. CONFIRMACIÓ I GUARDAT A BASE DE DADES ====================
    print("\n" + "="*70)
    print(" 💾 GUARDAR RESULTATS A LA BASE DE DADES")
    print("="*70)
    
    # Comptem les noves assignacions a l'històric
    assignacions_noves_historic = len(millor_solucio)
    
    print("\n📊 RESUM DE DADES A GUARDAR:")
    print("-" * 70)
    print(f"   • Assignacions a guardar a 'assig_grup_T': {len(millor_solucio)}")
    print(f"   • Assignacions a afegir a 'historic_assignacions': {assignacions_noves_historic}")
    print(f"   • Treballadors afectats: {len(assignacions_per_treb)}")
    print(f"   • Total hores assignades: {sum(hores_per_treb.values()) if hores_per_treb else 0:.1f}h")
    
    print("\n📁 FITXERS JA CREATS (es mantindran independentment de la resposta):")
    print(f"   • {fitxer_json}")
    print(f"   • {fitxer_csv}")
    print(f"   • {fitxer_stats}")
    if no_cobertes:
        print(f"   • {fitxer_no_cobertes}")
    
    print("\n" + "-" * 70)
    resposta = input("\n❓ Vols guardar aquests resultats a la base de dades? (S/N): ").strip().upper()
    
    if resposta in ['S', 'SI', 'SÍ', 'Y', 'YES']:
        print("\n💾 Guardant resultats a la base de dades...")
        print("-" * 70)
        
        # 1. Guardar assignacions a assig_grup_T
        if data_loader.guarda_assignacions_grup_T(millor_solucio, treballadors, calendari, necessitats):
            print(" ✓ Assignacions guardades a 'assig_grup_T'")
        else:
            print(" ✗ Error guardant assignacions a 'assig_grup_T'")
        
        # 2. Guardar històric actualitzat
        try:
            data_loader.guarda_historic(estadistiques, csv_path='historic_assignacions.csv')
            print(" ✓ Històric actualitzat a 'historic_assignacions'")
        except Exception as e:
            print(f" ✗ Error guardant històric: {e}")
        
        print("\n" + "="*70)
        print(" ✅ RESULTATS GUARDATS A LA BASE DE DADES")
        print("="*70)
        
    else:
        print("\n" + "="*70)
        print(" ℹ️  DADES NO GUARDADES A LA BASE DE DADES")
        print("="*70)
        print("\n   Els fitxers CSV/JSON s'han mantingut per a la teva consulta.")
        print("   No s'ha modificat la base de dades.")
    
    # Tancar la connexió a la base de dades
    data_loader.close()
    
    print("\n" + "="*70)
    print(" ✅ PROCÉS COMPLETAT")
    print("="*70)
    print()
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Executa el sistema d\'assignacions amb opcions d\'interval de dies')
    parser.add_argument('--start-date', '-s', help='Data d\'inici (YYYY-MM-DD o DD/MM/YYYY)')
    parser.add_argument('--end-date', '-e', help='Data final (YYYY-MM-DD o DD/MM/YYYY)')
    parser.add_argument('--on-duplicate', help="Com gestionar assignacions prèvies en les mateixes dates: 'replace_all' or 'add_new_only'")

    args = parser.parse_args()

    def parse_user_date(s):
        if not s:
            return None
        for fmt in ('%Y-%m-%d', '%d/%m/%Y'):
            try:
                return datetime.strptime(s, fmt).date()
            except Exception:
                continue
        raise ValueError(f"Format de data no vàlid: {s}")

    def ask_for_date_interval():
        """Pregunta a l'usuari per un interval de dates si no s'ha passat per CLI.
        L'usuari pot deixar en blanc per no filtrar una de les dues dates.
        """
        print('\nNo has passat cap interval per la línia de comandes.')
        print('Introdueix un interval opcional (format YYYY-MM-DD o DD/MM/YYYY).')
        print("Deixa en blanc i prem Enter per no filtrar (usar totes les dates).\n")

        while True:
            try:
                s_in = input('Data d\'inici (o Enter per no filtrar): ').strip()
                e_in = input('Data final (o Enter per no filtrar): ').strip()

                sd = parse_user_date(s_in) if s_in else None
                ed = parse_user_date(e_in) if e_in else None

                # Si cap de les dues s'ha definit, retornem (None, None)
                return sd, ed
            except ValueError as ve:
                print(f"Format invàlid: {ve}. Torna-ho a provar.\n")

    sd = parse_user_date(args.start_date) if args.start_date else None
    ed = parse_user_date(args.end_date) if args.end_date else None
    od = args.on_duplicate if args.on_duplicate else None

    # Si no s'han passat per CLI, demanem interactivament
    if sd is None and ed is None and (args.start_date is None and args.end_date is None):
        sd, ed = ask_for_date_interval()

    main(start_date=sd, end_date=ed, on_duplicate=od)