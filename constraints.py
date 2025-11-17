# constraints.py - ACTUALITZAT amb NOVES RESTRICCIONS

from typing import List, Dict, Set
from data_structures import (
    Assignacio, Treballador, Torn, NecessitatCobertura, 
    DiaCalendari, EstadistiquesGlobals
)
from collections import defaultdict
from datetime import timedelta, datetime, date

class RestriccionManager:
    def __init__(self):
        self.restriccions = []
    
    def afegeix_restriccio(self, funcio, pes: float, nom: str):
        """Afegeix una nova restricció al sistema"""
        self.restriccions.append({
            'funcio': funcio,
            'pes': pes,
            'nom': nom
        })
    
    def evalua_solucio(self, assignacions: List[Assignacio],
                       treballadors: Dict[str, Treballador],
                       torns: Dict[str, Torn],
                       necessitats: List[NecessitatCobertura],
                       calendari: Dict,
                       estadistiques: EstadistiquesGlobals = None) -> Dict:
        """
        Retorna un diccionari amb el score total i scores individuals
        """
        score_total = 0
        detall_scores = {}
        
        for restriccio in self.restriccions:
            try:
                score = restriccio['funcio'](
                    assignacions, treballadors, torns, 
                    necessitats, calendari, estadistiques
                )
                score_ponderat = score * restriccio['pes']
                score_total += score_ponderat
                detall_scores[restriccio['nom']] = {
                    'score': score,
                    'pes': restriccio['pes'],
                    'ponderat': score_ponderat
                }
            except Exception as e:
                print(f"Error en {restriccio['nom']}: {e}")
                detall_scores[restriccio['nom']] = {
                    'score': 0,
                    'pes': restriccio['pes'],
                    'ponderat': 0,
                    'error': str(e)
                }
        
        return {
            'total': score_total,
            'detall': detall_scores
        }

# ---------------------------
# Helpers
# ---------------------------

def _to_date(d):
    """
    Accepta un objecte que pot ser datetime.date o datetime.datetime o ja date.
    Retorna un datetime.date.
    """
    if isinstance(d, date) and not isinstance(d, datetime):
        return d
    if isinstance(d, datetime):
        return d.date()
    # Si és un string, intentar parsejar (fallback)
    try:
        # format habitual 'YYYY-MM-DD'
        return datetime.strptime(str(d), '%Y-%m-%d').date()
    except Exception:
        try:
            return datetime.strptime(str(d), '%d/%m/%Y').date()
        except Exception:
            # no podem parsejar bé; retornem None perquè la restricció la consideri violada
            return None


# ============= RESTRICCIONS CRÍTIQUES =============

def restriccio_grup_T(assignacions: List[Assignacio],
                     treballadors: Dict[str, Treballador],
                     torns: Dict[str, Torn],
                     necessitats: List[NecessitatCobertura],
                     calendari: Dict,
                     estadistiques: EstadistiquesGlobals = None) -> float:
    """
    CRÍTICA: Només treballadors del grup T poden fer substitucions
    """
    violations = 0
    total = len(assignacions)
    
    if total == 0:
        return 100
    
    for assign in assignacions:
        treballador = treballadors[assign.treballador_id]
        if treballador.grup != 'T':
            violations += 1
    
    return 100 * (1 - violations / total)


def restriccio_sense_descans(assignacions: List[Assignacio],
                            treballadors: Dict[str, Treballador],
                            torns: Dict[str, Torn],
                            necessitats: List[NecessitatCobertura],
                            calendari: Dict,
                            estadistiques: EstadistiquesGlobals = None) -> float:
    """
    CRÍTICA: Els treballadors NO poden treballar els seus dies de descans
    """
    violations = 0
    total = len(assignacions)
    
    if total == 0:
        return 100
    
    for assign in assignacions:
        treballador = treballadors[assign.treballador_id]
        if treballador.te_descans(assign.data):
            violations += 1
    
    return 100 * (1 - violations / total)


def restriccio_formacio_requerida(assignacions: List[Assignacio],
                                  treballadors: Dict[str, Treballador],
                                  torns: Dict[str, Torn],
                                  necessitats: List[NecessitatCobertura],
                                  calendari: Dict,
                                  estadistiques: EstadistiquesGlobals = None) -> float:
    """
    El treballador ha de tenir la formació/habilitació necessària
    """
    necessitats_map = {(nec.servei, nec.data): nec for nec in necessitats}
    
    violations = 0
    total = len(assignacions)
    
    if total == 0:
        return 100
    
    for assign in assignacions:
        treballador = treballadors[assign.treballador_id]
        key = (assign.torn_id, assign.data)
        
        if key in necessitats_map:
            nec = necessitats_map[key]
            if not nec.formacio.intersection(treballador.habilitacions):
                violations += 1
    
    return 100 * (1 - violations / total)


def restriccio_linia_correcta(assignacions: List[Assignacio],
                              treballadors: Dict[str, Treballador],
                              torns: Dict[str, Torn],
                              necessitats: List[NecessitatCobertura],
                              calendari: Dict,
                              estadistiques: EstadistiquesGlobals = None) -> float:
    """
    El treballador ha d'estar habilitat per la línia del torn
    """
    necessitats_map = {(nec.servei, nec.data): nec for nec in necessitats}
    
    violations = 0
    total = len(assignacions)
    
    if total == 0:
        return 100
    
    for assign in assignacions:
        treballador = treballadors[assign.treballador_id]
        key = (assign.torn_id, assign.data)
        
        if key in necessitats_map:
            nec = necessitats_map[key]
            if treballador.linia != nec.linia:
                violations += 1
    
    return 100 * (1 - violations / total)


def restriccio_hores_anuals(assignacions: List[Assignacio],
                           treballadors: Dict[str, Treballador],
                           torns: Dict[str, Torn],
                           necessitats: List[NecessitatCobertura],
                           calendari: Dict,
                           estadistiques: EstadistiquesGlobals = None) -> float:
    """
    CRÍTICA: Els treballadors no poden superar les 1.605h anuals
    BONUS: Prioritzar treballadors que encara estiguin dins les 1.218h estàndard
    """
    hores_per_treballador = defaultdict(float)
    
    # Sumem les hores d'aquesta solució
    for assign in assignacions:
        hores_per_treballador[assign.treballador_id] += assign.durada_hores
    
    violations = 0
    bonus_dins_estandard = 0
    total = len(set(a.treballador_id for a in assignacions))
    
    if total == 0:
        return 100
    
    for treb_id in hores_per_treballador:
        treballador = treballadors[treb_id]
        hores_totals = treballador.hores_anuals_realitzades + hores_per_treballador[treb_id]
        
        # Violació crítica: supera el màxim ampliable
        if hores_totals > treballador.max_hores_ampliables:
            violations += 1
        # Bonus: està dins l'estàndard
        elif hores_totals <= treballador.max_hores_anuals:
            bonus_dins_estandard += 1
    
    # Score base: no violar el màxim
    score_base = 100 * (1 - violations / total) if total > 0 else 100
    
    # Bonus: prioritzar treballadors dins l'estàndard (fins a +10 punts)
    bonus = (bonus_dins_estandard / total) * 10 if total > 0 else 0
    
    return min(100, score_base + bonus)


# ============= RESTRICCIONS DE DESCANSOS I HORARIS =============

def restriccio_unica_assignacio_per_dia_rigida(assignacions: List[Assignacio],
                                              treballadors: Dict[str, Treballador],
                                              torns: Dict[str, Torn],
                                              necessitats: List[NecessitatCobertura],
                                              calendari: Dict,
                                              estadistiques: EstadistiquesGlobals = None) -> float:
    """
    RÍGIDA: Assegura que cada treballador tingui com a màxim UNA assignació per dia (independentment
    de l'hora o solapaments). Si es detecta qualsevol treballador amb >1 assignació en el mateix dia,
    retorna 0 per invalidar la solució.
    Aquesta funció normalitza la data amb _to_date() per evitar problemes de tipus datetime/date.
    """
    assigns_per_treb_dia = defaultdict(set)  # (treb_id -> set(dates))
    for a in assignacions:
        treb = a.treballador_id
        d = _to_date(a.data)
        if d is None:
            # no podem determinar la data correctament => considerem violació
            return 0
        # Si ja existeix la mateixa data, violació
        if d in assigns_per_treb_dia[treb]:
            return 0
        assigns_per_treb_dia[treb].add(d)

    # També comprovem l'última assignació de l'històric (si existeix): no es pot assignar el mateix dia
    if estadistiques:
        for treb_id in list(assigns_per_treb_dia.keys()):
            hist = estadistiques.get_historic(treb_id)
            if hist and getattr(hist, 'ultima_assignacio', None):
                ultima = hist.ultima_assignacio
                ultima_data = _to_date(ultima.data)
                if ultima_data in assigns_per_treb_dia[treb_id]:
                    return 0

    return 100

def restriccio_sense_solapaments_rigida(assignacions, treballadors, torns, necessitats, calendari, estadistiques=None):
    """
    RÍGIDA: Un treballador no pot tenir dos torns el mateix dia.
    Si es detecta solapament, retorna 0 immediatament.
    """
    # Agrupem per treballador i dia (normalitzat)
    assigns_per_treb_dia = defaultdict(list)

    for assign in assignacions:
        d = _to_date(assign.data)
        if d is None:
            return 0
        assigns_per_treb_dia[(assign.treballador_id, d)].append(assign)

    # Afegim històric d'última assignació si cal
    if estadistiques:
        for (treb_id, d), lst in list(assigns_per_treb_dia.items()):
            hist = estadistiques.get_historic(treb_id)
            if hist and getattr(hist, 'ultima_assignacio', None):
                ultima = hist.ultima_assignacio
                ultima_d = _to_date(ultima.data)
                if ultima_d == d:
                    lst.insert(0, ultima)

    for (treb_id, d), assigns in assigns_per_treb_dia.items():
        if len(assigns) < 2:
            continue
        # Ordenem per hora d'inici
        assigns_ordenades = sorted(assigns, key=lambda a: (a.data, a.hora_inici))
        for i in range(1, len(assigns_ordenades)):
            a1 = assigns_ordenades[i - 1]
            a2 = assigns_ordenades[i]
            fi1 = a1.hora_fi_real()
            inici2 = datetime.combine(_to_date(a2.data), a2.hora_inici)
            # conversió fi1: assumim retorn datetime; si no, proveir fallback
            if isinstance(fi1, datetime):
                pass
            else:
                # si fi1 és hora (time), crear datetime amb la mateixa data
                fi1 = datetime.combine(_to_date(a1.data), fi1)
            # Comprovació de no-solapament
            if not (fi1 <= inici2):
                return 0
    return 100

def restriccio_dies_consecutius(assignacions: List[Assignacio],
                               treballadors: Dict[str, Treballador],
                               torns: Dict[str, Torn],
                               necessitats: List[NecessitatCobertura],
                               calendari: Dict,
                               estadistiques: EstadistiquesGlobals = None) -> float:
    """
    IMPORTANT: Màxim 9 dies consecutius treballats
    """
    # Agrupem per treballador
    assigns_per_treb = defaultdict(list)
    for a in assignacions:
        assigns_per_treb[a.treballador_id].append(a.data)
    
    # Afegim les dates de l'històric si existeix
    if estadistiques:
        for treb_id in assigns_per_treb:
            historic = estadistiques.get_historic(treb_id)
            assigns_per_treb[treb_id].extend([a.data for a in historic.assignacions_any])
    
    violations = 0
    total = len(assigns_per_treb)
    
    if total == 0:
        return 100
    
    for treb_id, dates in assigns_per_treb.items():
        dates_ordenades = sorted(set(dates))
        
        consecutius = 1
        max_consecutius = 1
        
        for i in range(1, len(dates_ordenades)):
            if (dates_ordenades[i] - dates_ordenades[i-1]).days == 1:
                consecutius += 1
                max_consecutius = max(max_consecutius, consecutius)
            else:
                consecutius = 1
        
        if max_consecutius > 9:
            violations += (max_consecutius - 9)
    
    # Penalitzem proporcionalment
    max_violations = total * 5  # Assumim màxim 5 dies d'excés
    score = max(0, 100 - (violations / max_violations * 100)) if max_violations > 0 else 100
    
    return score


def restriccio_descans_minim_12h_rigida(assignacions: List[Assignacio],
                                        treballadors: Dict[str, Treballador],
                                        torns: Dict[str, Torn],
                                        necessitats: List[NecessitatCobertura],
                                        calendari: Dict,
                                        estadistiques: EstadistiquesGlobals = None) -> float:
    """
    RÍGIDA: Mínim 12 hores de descans entre torns consecutius.
    Si hi ha una sola violació, retorna 0.
    Aquesta versió normalitza dates i comprova l'última assignació d'històric.
    """
    assigns_per_treb = defaultdict(list)
    for a in assignacions:
        assigns_per_treb[a.treballador_id].append(a)

    for treb_id, assigns in assigns_per_treb.items():
        # afegim última assignació de l'històric (si existeix)
        if estadistiques:
            hist = estadistiques.get_historic(treb_id)
            if hist and getattr(hist, 'ultima_assignacio', None):
                assigns = [hist.ultima_assignacio] + assigns

        # Ordenem per data + hora d'inici (ús de la data normalitzada per evitar inconsistències)
        try:
            assigns_ordenades = sorted(assigns, key=lambda a: (_to_date(a.data), a.hora_inici))
        except Exception:
            # si no es pot ordenar correctament, considerem la solució invàlida
            return 0

        for i in range(1, len(assigns_ordenades)):
            a1 = assigns_ordenades[i - 1]
            a2 = assigns_ordenades[i]
            fi_a1 = a1.hora_fi_real()
            # assegurar que fi_a1 és datetime
            if not isinstance(fi_a1, datetime):
                fi_a1 = datetime.combine(_to_date(a1.data), fi_a1)
            inici_a2 = datetime.combine(_to_date(a2.data), a2.hora_inici)
            hores_descans = (inici_a2 - fi_a1).total_seconds() / 3600.0
            if hores_descans < 12:
                return 0
    return 100


def restriccio_divendres_cap_setmana_rigida(assignacions: List[Assignacio],
                                            treballadors: Dict[str, Treballador],
                                            torns: Dict[str, Torn],
                                            necessitats: List[NecessitatCobertura],
                                            calendari: Dict,
                                            estadistiques: EstadistiquesGlobals = None) -> float:
    """
    RÍGIDA: Si un treballador té descans dissabte i diumenge,
    el divendres no pot acabar més tard de les 22:00h.
    Si hi ha violació, retorna 0.
    """
    for a in assignacions:
        d = _to_date(a.data)
        if d is None:
            return 0
        # només divendres
        if d.weekday() != 4:
            continue
        treballador = treballadors.get(a.treballador_id)
        if not treballador:
            return 0
        dissabte = d + timedelta(days=1)
        diumenge = d + timedelta(days=2)
        # Si té descans dissabte i diumenge (segons la funció te_descans)
        try:
            te_descans_dissabte = treballador.te_descans(dissabte)
            te_descans_diumenge = treballador.te_descans(diumenge)
        except Exception:
            # en cas d'errors amb el model Treballador, considerem violació
            return 0

        if te_descans_dissabte and te_descans_diumenge:
            # si creua mitjanit o acaba després de 22:00 -> violació
            if a.hora_fi < a.hora_inici:
                return 0
            if a.hora_fi.hour > 22 or (a.hora_fi.hour == 22 and a.hora_fi.minute > 0):
                return 0
    return 100


# ============= RESTRICCIONS D'EQUITAT =============

def restriccio_equitat_canvis_zona(assignacions: List[Assignacio],
                                   treballadors: Dict[str, Treballador],
                                   torns: Dict[str, Torn],
                                   necessitats: List[NecessitatCobertura],
                                   calendari: Dict,
                                   estadistiques: EstadistiquesGlobals = None) -> float:
    """
    BONUS: Distribució equitativa dels canvis de zona entre treballadors
    Objectiu: minimitzar la desviació estàndard
    """
    necessitats_map = {(nec.servei, nec.data): nec for nec in necessitats}
    
    canvis_per_treballador = defaultdict(int)
    
    # Comptem els canvis en aquesta solució
    for assign in assignacions:
        treballador = treballadors[assign.treballador_id]
        key = (assign.torn_id, assign.data)
        
        if key in necessitats_map:
            nec = necessitats_map[key]
            if treballador.es_canvi_zona(nec.zona):
                canvis_per_treballador[assign.treballador_id] += 1
    
    # Afegim els canvis de l'històric
    for treb_id in canvis_per_treballador:
        canvis_per_treballador[treb_id] += treballadors[treb_id].canvis_zona
    
    if not canvis_per_treballador:
        return 100
    
    # Calculem desviació estàndard
    valors = list(canvis_per_treballador.values())
    mitjana = sum(valors) / len(valors)
    variancia = sum((v - mitjana) ** 2 for v in valors) / len(valors)
    desviacio = variancia ** 0.5
    
    # Normalitzem: desviació 0 = 100, desviació >3 = 0
    score = max(0, 100 - (desviacio / 3 * 100))
    
    return score


def restriccio_equitat_canvis_torn(assignacions: List[Assignacio],
                                   treballadors: Dict[str, Treballador],
                                   torns: Dict[str, Torn],
                                   necessitats: List[NecessitatCobertura],
                                   calendari: Dict,
                                   estadistiques: EstadistiquesGlobals = None) -> float:
    """
    BONUS: Distribució equitativa dels canvis de torn entre treballadors
    Objectiu: minimitzar la desviació estàndard
    """
    necessitats_map = {(nec.servei, nec.data): nec for nec in necessitats}
    
    canvis_per_treballador = defaultdict(int)
    
    # Comptem els canvis en aquesta solució
    for assign in assignacions:
        treballador = treballadors[assign.treballador_id]
        key = (assign.torn_id, assign.data)
        
        if key in necessitats_map:
            nec = necessitats_map[key]
            if treballador.es_canvi_torn(nec.torn):
                canvis_per_treballador[assign.treballador_id] += 1
    
    # Afegim els canvis de l'històric
    for treb_id in canvis_per_treballador:
        canvis_per_treballador[treb_id] += treballadors[treb_id].canvis_torn
    
    if not canvis_per_treballador:
        return 100
    
    # Calculem desviació estàndard
    valors = list(canvis_per_treballador.values())
    mitjana = sum(valors) / len(valors)
    variancia = sum((v - mitjana) ** 2 for v in valors) / len(valors)
    desviacio = variancia ** 0.5
    
    # Normalitzem: desviació 0 = 100, desviació >3 = 0
    score = max(0, 100 - (desviacio / 3 * 100))
    
    return score


def restriccio_cobertura_completa(assignacions: List[Assignacio],
                                  treballadors: Dict[str, Treballador],
                                  torns: Dict[str, Torn],
                                  necessitats: List[NecessitatCobertura],
                                  calendari: Dict,
                                  estadistiques: EstadistiquesGlobals = None) -> float:
    """
    Totes les necessitats de cobertura han d'estar assignades
    """
    assignacions_set = set((a.torn_id, a.data) for a in assignacions)
    
    cobertes = 0
    total_necessitats = len(necessitats)
    
    if total_necessitats == 0:
        return 100
    
    for nec in necessitats:
        key = (nec.servei, nec.data)
        if key in assignacions_set:
            cobertes += 1
    
    return 100 * (cobertes / total_necessitats)


def restriccio_distribucio_equilibrada(assignacions: List[Assignacio],
                                       treballadors: Dict[str, Treballador],
                                       torns: Dict[str, Torn],
                                       necessitats: List[NecessitatCobertura],
                                       calendari: Dict,
                                       estadistiques: EstadistiquesGlobals = None) -> float:
    """
    Evitar que uns treballadors tinguin moltes assignacions i altres poques
    """
    assignacions_per_treballador = defaultdict(int)
    
    for assign in assignacions:
        assignacions_per_treballador[assign.treballador_id] += 1
    
    if not assignacions_per_treballador:
        return 100
    
    valors = list(assignacions_per_treballador.values())
    mitjana = sum(valors) / len(valors)
    desviacio = sum(abs(v - mitjana) for v in valors) / len(valors)
    
    # Normalitzem: menys desviació = millor score
    # Assumim que més de 5 de desviació és molt dolent
    score = max(0, 100 - (desviacio * 10))
    
    return score


