# data_structures.py - SIMPLIFICAT AMB DATES DIRECTES

from dataclasses import dataclass, field
from typing import List, Dict, Set, Optional
from datetime import datetime, time, date, timedelta


@dataclass
class Treballador:
    id: str
    nom: str
    plaza: str  # IGAIG3R, SAASA1M...
    torn_assignat: str  # Rotatiu, Matí, Tarda, Nit... (torn predeterminat)
    zona: str  # Igualada, Sant Andreu... (zona predeterminada)
    habilitacions: Set[str]  # AE, AE+C, ME...
    linia: str  # LA, LB...
    categoria: str  # 3, 4...
    grup: str  # A, B, T... (IMPORTANT: només grup T per assignacions)
    denominacio: str  # AE tipus 1, AE tipus 2/3...

    # Descansos - SIMPLIFICAT: només dates
    dates_descans: Set[date] = field(default_factory=set)

    # Control anual d'hores
    hores_anuals_realitzades: float = 0.0  # Hores ja fetes en l'any
    max_hores_anuals: float = 1218.0  # Límit estàndard
    max_hores_ampliables: float = 1605.0  # Límit màxim ampliable

    # Estadístiques de canvis (per equitat)
    canvis_zona: int = 0  # Nombre de vegades que ha treballat fora de la seva zona
    canvis_torn: int = 0  # Nombre de vegades que ha treballat en torn diferent

    def te_descans(self, data: date) -> bool:
        """Comprova si el treballador té descans en una data"""
        return data in self.dates_descans

    def pot_cobrir_torn(self, torn: 'Torn') -> bool:
        """Comprova si el treballador pot cobrir un torn (per línia, grup, etc)"""
        # CRÍTICA: Només treballadors del grup T poden fer substitucions
        if self.grup != 'T':
            return False
        return self.linia == torn.linia

    def hores_disponibles(self) -> float:
        """Retorna quantes hores té disponibles encara aquest any"""
        return self.max_hores_ampliables - self.hores_anuals_realitzades

    def esta_dins_limit_estandard(self) -> bool:
        """Comprova si està dins del límit estàndard (abans d'ampliar)"""
        return self.hores_anuals_realitzades <= self.max_hores_anuals

    def es_canvi_zona(self, zona: str) -> bool:
        """Comprova si treballar en aquesta zona seria un canvi"""
        return self.zona != zona

    def es_canvi_torn(self, torn: str) -> bool:
        """Comprova si treballar en aquest torn seria un canvi"""
        # La propietat `torn_assignat` pot contenir valors com:
        # 'Mati', 'Tarda', 'Nit', 'Matí,Nit', 'Tarda, Nit', etc.
        # El paràmetre `torn` pot ser:
        # - una string amb el nom del torn ('Matí', 'Tarda', 'Nit', o combinacions)
        # - un objecte amb atribut `hora_inici` (ServeiTorn o Assignacio)
        # - un objecte `time` amb l'hora d'inici

        def normalize(s: str) -> str:
            return s.strip().lower().replace('í', 'i').replace('\u00A0', ' ')

        # Conjunt d'opcions del treballador (normalitzat)
        treballador_opts = set(p.strip().lower().replace('í', 'i') for p in self.torn_assignat.split(',') if p.strip())

        # Determina el nom del torn segons el paràmetre
        torn_nom = None
        from datetime import time as _time

        if isinstance(torn, str):
            # Si la cadena conté múltiples opcions, comprovem la intersecció
            parts = [p.strip().lower().replace('í', 'i') for p in torn.split(',') if p.strip()]
            if parts:
                # Si hi ha superposició entre les opcions del torn i les del treballador,
                # considerem que NO és canvi
                if treballador_opts.intersection(parts):
                    return False
                # En cas contrari, comparem amb la primera opció del torn
                torn_nom = parts[0]
        elif isinstance(torn, _time):
            h = torn.hour
            if h < 12:
                torn_nom = 'mati'
            elif h >= 20:
                torn_nom = 'nit'
            else:
                torn_nom = 'tarda'
        else:
            # Objecte amb possible atribut hora_inici
            hora = None
            if hasattr(torn, 'hora_inici'):
                hora = getattr(torn, 'hora_inici')
            elif hasattr(torn, 'hora'):
                hora = getattr(torn, 'hora')

            if hora is not None:
                if isinstance(hora, _time):
                    h = hora.hour
                else:
                    try:
                        h = int(str(hora).split(':')[0])
                    except Exception:
                        h = None

                if h is not None:
                    if h < 12:
                        torn_nom = 'mati'
                    elif h >= 20:
                        torn_nom = 'nit'
                    else:
                        torn_nom = 'tarda'

        # Si no hem pogut deduir el nom, fem comparació directa amb la cadena original
        if not torn_nom:
            return normalize(self.torn_assignat) != normalize(str(torn))

        # Finalment, comprovem si el torn determinat està entre les opcions del treballador
        return torn_nom not in treballador_opts


@dataclass
class Torn:
    """Representa un torn amb els seus múltiples serveis"""
    id: str  # AAL1, AAL2, ABO0...
    linia: str  # LA, LB...
    zona: str  # F, G...
    serveis: Dict[int, 'ServeiTorn']  # {1: ServeiTorn, 2: ServeiTorn...}


@dataclass
class ServeiTorn:
    """Un servei dins d'un torn (S1, S2, S3, S4)"""
    num_servei: int  # 1, 2, 3, 4
    codis_servei: Set[str]  # {'000', '100', '200', '300'}
    hora_inici: time
    hora_fi: time
    creua_mitjanit: bool  # True si hora_fi < hora_inici

    def durada_hores(self) -> float:
        """Calcula la durada del servei en hores"""
        if self.creua_mitjanit:
            # Creua mitjanit: (24 - hora_inici) + hora_fi
            minuts_primer_dia = (24 - self.hora_inici.hour) * 60 - self.hora_inici.minute
            minuts_segon_dia = self.hora_fi.hour * 60 + self.hora_fi.minute
            total_minuts = minuts_primer_dia + minuts_segon_dia
        else:
            # Normal
            inici_minuts = self.hora_inici.hour * 60 + self.hora_inici.minute
            fi_minuts = self.hora_fi.hour * 60 + self.hora_fi.minute
            total_minuts = fi_minuts - inici_minuts
        return total_minuts / 60.0


@dataclass
class DiaCalendari:
    data: date
    servei_bv: str  # '000', '100', '504'...
    dia_setmana: str  # Dx, Dj, Dv...
    dia_mes: str  # 1 Gener, 2 Gener...
    dia_num: str  # D001, D002...

    def es_divendres(self) -> bool:
        """Comprova si és divendres"""
        return self.data.weekday() == 4  # 0=dilluns, 4=divendres

    def es_dissabte(self) -> bool:
        """Comprova si és dissabte"""
        return self.data.weekday() == 5

    def es_diumenge(self) -> bool:
        """Comprova si és diumenge"""
        return self.data.weekday() == 6


@dataclass
class Assignacio:
    treballador_id: str
    torn_id: str
    data: date
    hora_inici: time
    hora_fi: time
    durada_hores: float = 0.0  # Calculada automàticament
    es_canvi_zona: bool = False  # Si és fora de la seva zona
    es_canvi_torn: bool = False  # Si és fora del seu torn habitual
    apunt_data: Optional[datetime] = None  # Timestamp de quan es va apuntar l'assignació al historic

    def __hash__(self):
        return hash((self.treballador_id, self.torn_id, self.data))

    def __eq__(self, other):
        return (self.treballador_id == other.treballador_id and
                self.torn_id == other.torn_id and
                self.data == other.data)

    def hora_fi_real(self) -> datetime:
        """Retorna la data/hora real de finalització (considerant creuar mitjanit)"""
        dt = datetime.combine(self.data, self.hora_fi)
        if self.hora_fi < self.hora_inici:
            # Creua mitjanit, afegim un dia
            dt += timedelta(days=1)
        return dt


@dataclass
class NecessitatCobertura:
    """Representa un torn que necessita ser cobert"""
    servei: str  # Codi del servei (AIG2, AML1...)
    residencia: str  # IG, ML...
    torn: str  # Matí, Tarda, Nit, Rotatiu...
    formacio: Set[str]  # AE, ME, AE+C...
    linia: str  # LA, LB...
    zona: str  # H, G, F...
    motiu: str  # "Té descans", "Malaltia"...
    data: date

    def __hash__(self):
        return hash((self.servei, self.data))

    def __eq__(self, other):
        return self.servei == other.servei and self.data == other.data


@dataclass
class HistoricTreballador:
    """Històric d'assignacions d'un treballador"""
    treballador_id: str
    ultima_assignacio: Optional[Assignacio] = None
    assignacions_any: List[Assignacio] = field(default_factory=list)

    def afegir_assignacio(self, assignacio: Assignacio):
        """Afegeix una assignació al històric"""
        self.assignacions_any.append(assignacio)
        self.ultima_assignacio = assignacio

    def hora_fi_ultim_torn(self) -> Optional[datetime]:
        """Retorna la hora de finalització de l'últim torn"""
        if self.ultima_assignacio:
            return self.ultima_assignacio.hora_fi_real()
        return None

    def ultim_torn_realitzat(self) -> Optional[str]:
        """Retorna l'ID de l'últim torn realitzat"""
        if self.ultima_assignacio:
            return self.ultima_assignacio.torn_id
        return None

    def total_hores_any(self) -> float:
        """Calcula el total d'hores treballades aquest any"""
        return sum(a.durada_hores for a in self.assignacions_any)

    def dies_consecutius_treballats(self) -> int:
        """Calcula el màxim de dies consecutius treballats"""
        if not self.assignacions_any:
            return 0

        dates = sorted(set(a.data for a in self.assignacions_any))
        max_consecutius = 1
        consecutius_actual = 1

        for i in range(1, len(dates)):
            if (dates[i] - dates[i-1]).days == 1:
                consecutius_actual += 1
                max_consecutius = max(max_consecutius, consecutius_actual)
            else:
                consecutius_actual = 1

        return max_consecutius

    def total_canvis_zona(self) -> int:
        """Compte el total de canvis de zona"""
        return sum(1 for a in self.assignacions_any if a.es_canvi_zona)

    def total_canvis_torn(self) -> int:
        """Compte el total de canvis de torn"""
        return sum(1 for a in self.assignacions_any if a.es_canvi_torn)


@dataclass
class EstadistiquesGlobals:
    """Estadístiques globals per avaluar l'equitat"""
    historials: Dict[str, HistoricTreballador] = field(default_factory=dict)

    def get_historic(self, treballador_id: str) -> HistoricTreballador:
        """Obté o crea l'històric d'un treballador"""
        if treballador_id not in self.historials:
            self.historials[treballador_id] = HistoricTreballador(treballador_id)
        return self.historials[treballador_id]

    def mitjana_canvis_zona(self) -> float:
        """Calcula la mitjana de canvis de zona entre tots els treballadors"""
        if not self.historials:
            return 0.0
        return sum(h.total_canvis_zona() for h in self.historials.values()) / len(self.historials)

    def mitjana_canvis_torn(self) -> float:
        """Calcula la mitjana de canvis de torn entre tots els treballadors"""
        if not self.historials:
            return 0.0
        return sum(h.total_canvis_torn() for h in self.historials.values()) / len(self.historials)

    def desviacio_canvis_zona(self) -> float:
        """Calcula la desviació estàndard dels canvis de zona"""
        if not self.historials:
            return 0.0
        mitjana = self.mitjana_canvis_zona()
        variancia = sum((h.total_canvis_zona() - mitjana) ** 2 for h in self.historials.values()) / len(self.historials)
        return variancia ** 0.5

    def desviacio_canvis_torn(self) -> float:
        """Calcula la desviació estàndard dels canvis de torn"""
        if not self.historials:
            return 0.0
        mitjana = self.mitjana_canvis_torn()
        variancia = sum((h.total_canvis_torn() - mitjana) ** 2 for h in self.historials.values()) / len(self.historials)
        return variancia ** 0.5
