# genetic_algorithm.py - CORREGIT AMB REPARACIÓ INTEL·LIGENT

import random
from typing import List, Dict, Tuple
from datetime import datetime
from data_structures import (
    Assignacio, Treballador, Torn, NecessitatCobertura, 
    DiaCalendari, ServeiTorn, EstadistiquesGlobals
)
from constraints import RestriccionManager
from data_loader import DataLoader

class AlgorismeGenetic:
    def __init__(self, 
                 treballadors: Dict[str, Treballador],
                 torns: Dict[str, Torn],
                 necessitats: List[NecessitatCobertura],
                 calendari: Dict,
                 restriccions: RestriccionManager,
                 estadistiques: EstadistiquesGlobals,
                 mida_poblacio: int = 50,
                 exclude_map: Dict = None):
        self.treballadors = treballadors
        self.torns = torns
        self.necessitats = necessitats
        self.calendari = calendari
        self.restriccions = restriccions
        self.estadistiques = estadistiques
        self.mida_poblacio = mida_poblacio

        # exclude_map: opcional, map de date -> set(treballador_id) per excloure
        self.exclude_map = exclude_map or {}

        # Filtrem només treballadors del grup T
        self.treballadors_grup_t = {
            tid: t for tid, t in treballadors.items() if t.grup == 'T'
        }

        print(f"   Treballadors grup T disponibles: {len(self.treballadors_grup_t)}")

        # Creem un índex ràpid de necessitats per facilitar la cerca
        self.necessitats_per_data = {}
        for nec in necessitats:
            if nec.data not in self.necessitats_per_data:
                self.necessitats_per_data[nec.data] = []
            self.necessitats_per_data[nec.data].append(nec)
    
    def _compleix_descans_12h(self, treb_id: str, data_nova, hora_inici_nova, 
                              assignacions_actuals: List[Assignacio]) -> bool:
        """
        Verifica que hi hagi 12h de descans des de l'última assignació
        """
        # Comprovem amb l'històric
        historic = self.estadistiques.get_historic(treb_id)
        
        ultimes_assignacions = []
        if historic and historic.assignacions_any:
            ultimes_assignacions.extend(historic.assignacions_any[-10:])  # Últimes 10
        
        # Afegim assignacions actuals d'aquest treballador
        ultimes_assignacions.extend([a for a in assignacions_actuals if a.treballador_id == treb_id])
        
        if not ultimes_assignacions:
            return True
        
        # Ordenem per data
        ultimes_assignacions.sort(key=lambda a: (a.data, a.hora_inici))
        
        inici_nova = datetime.combine(data_nova, hora_inici_nova)
        
        # Comprovem només amb les assignacions properes en el temps
        for assign_anterior in reversed(ultimes_assignacions):
            # Si l'assignació anterior és més de 2 dies abans, no cal comprovar
            if (data_nova - assign_anterior.data).days > 2:
                continue
                
            fi_anterior = assign_anterior.hora_fi_real()
            if not isinstance(fi_anterior, datetime):
                fi_anterior = datetime.combine(assign_anterior.data, fi_anterior)
            
            hores_descans = (inici_nova - fi_anterior).total_seconds() / 3600
            
            if hores_descans < 12:
                return False
        
        return True
    
    def genera_solucio_aleatoria(self) -> List[Assignacio]:
        """
        Genera una solució inicial amb filtres intel·ligents i validacions rígides
        """
        assignacions = []
        # CONTROL RÍGID: Un treballador només pot tenir una assignació per dia
        treballadors_per_dia = {}  # {(treballador_id, data): True}
        
        for necessitat in self.necessitats:
            # Busquem el torn corresponent
            if necessitat.servei not in self.torns:
                continue
            
            torn = self.torns[necessitat.servei]
            
            # Intentem trobar l'horari correcte per aquesta data
            try:
                servei = DataLoader.troba_servei_per_data(
                    torn, 
                    necessitat.data, 
                    self.calendari
                )
            except:
                continue
            
            # Creem una llista de treballadors candidats (només grup T)
            candidats = []

            for treb_id, treb in self.treballadors_grup_t.items():
                # VALIDACIÓ RÍGIDA 1: No pot tenir ja una assignació aquest dia
                if (treb_id, necessitat.data) in treballadors_per_dia:
                    continue
                
                # Filtre 0: Excloem si en aquesta data el treballador ja tenia assignació (opció add_new_only)
                if necessitat.data in self.exclude_map and treb_id in self.exclude_map[necessitat.data]:
                    continue

                # Filtre 1: No pot tenir descans
                if treb.te_descans(necessitat.data):
                    continue

                # Filtre 2: Ha de ser de la mateixa línia
                if treb.linia != necessitat.linia:
                    continue

                # Filtre 3: Ha de tenir la formació necessària
                if not necessitat.formacio.intersection(treb.habilitacions):
                    continue

                # Filtre 4: No pot superar hores anuals màximes
                hores_necessaries = servei.durada_hores()
                if treb.hores_anuals_realitzades + hores_necessaries > treb.max_hores_ampliables:
                    continue

                # VALIDACIÓ RÍGIDA 2: Ha de complir 12h de descans
                if not self._compleix_descans_12h(treb_id, necessitat.data, servei.hora_inici, assignacions):
                    continue

                candidats.append(treb_id)

            if not candidats:
                continue

            # Prioritzem treballadors amb menys assignacions i dins hores estàndard
            candidats_prioritzats = []

            for treb_id in candidats:
                treb = self.treballadors_grup_t[treb_id]
                prioritat = 0

                # Bonus si està dins hores estàndard
                if treb.esta_dins_limit_estandard():
                    prioritat += 10

                # Bonus si és la seva zona (menys canvis)
                if not treb.es_canvi_zona(necessitat.zona):
                    prioritat += 5

                # Bonus si és el seu torn (menys canvis)
                if not treb.es_canvi_torn(necessitat.torn):
                    prioritat += 5

                # Penalització per cada assignació que ja té (equilibri)
                num_assignacions = sum(1 for a in assignacions if a.treballador_id == treb_id)
                prioritat -= num_assignacions * 2

                candidats_prioritzats.append((treb_id, prioritat))

            # Ordenem per prioritat i triem amb pes aleatori
            candidats_prioritzats.sort(key=lambda x: x[1], reverse=True)

            # Selecció estocàstica: més probabilitat pels millors
            pesos = [max(1, c[1]) for c in candidats_prioritzats[:10]]
            treballador_escollit = random.choices(
                [c[0] for c in candidats_prioritzats[:10]], 
                weights=pesos, 
                k=1
            )[0]

            # Creem l'assignació
            treb = self.treballadors_grup_t[treballador_escollit]

            assignacio = Assignacio(
                treballador_id=treballador_escollit,
                torn_id=necessitat.servei,
                data=necessitat.data,
                hora_inici=servei.hora_inici,
                hora_fi=servei.hora_fi,
                durada_hores=servei.durada_hores(),
                es_canvi_zona=treb.es_canvi_zona(necessitat.zona),
                es_canvi_torn=treb.es_canvi_torn(necessitat.torn)
            )

            assignacions.append(assignacio)
            # REGISTREM que aquest treballador ja té assignació aquest dia
            treballadors_per_dia[(treballador_escollit, necessitat.data)] = True
        
        return assignacions
    
    def genera_poblacio_inicial(self) -> List[Tuple[List[Assignacio], Dict]]:
        """Genera la població inicial amb diversitat"""
        poblacio = []
        
        print(f"   Generant població inicial de {self.mida_poblacio} individus...")
        
        for i in range(self.mida_poblacio):
            solucio = self.genera_solucio_aleatoria()
            
            # Afegim variació aleatòria progressiva
            if i > 0:
                prob_mutacio = 0.1 + (i / self.mida_poblacio * 0.3)
                solucio = self.mutacio(solucio, prob_mutacio=prob_mutacio)
            
            resultat = self.restriccions.evalua_solucio(
                solucio, self.treballadors, self.torns, 
                self.necessitats, self.calendari, self.estadistiques
            )
            
            poblacio.append((solucio, resultat))
            
            if (i + 1) % 10 == 0:
                print(f"      {i + 1}/{self.mida_poblacio} individus generats")
        
        return poblacio
    
    def seleccio_torneig(self, poblacio: List[Tuple], 
                         mida_torneig: int = 3) -> List[Assignacio]:
        """Selecciona un individu per torneig"""
        torneig = random.sample(poblacio, min(mida_torneig, len(poblacio)))
        return max(torneig, key=lambda x: x[1]['total'])[0]
    
    def encreuament(self, pare1: List[Assignacio], 
                   pare2: List[Assignacio]) -> List[Assignacio]:
        """
        Encreuament intel·ligent: manté assignacions per necessitat
        VALIDACIÓ: Assegura que no hi hagi duplicats de treballador-dia
        """
        if len(pare1) == 0 or len(pare2) == 0:
            return pare1 if len(pare1) > 0 else pare2
        
        fill = []
        treballadors_per_dia = {}  # Control de duplicats
        
        # Per cada necessitat, triem l'assignació del pare1 o pare2
        for necessitat in self.necessitats:
            assign_pare1 = None
            assign_pare2 = None
            
            for a in pare1:
                if a.torn_id == necessitat.servei and a.data == necessitat.data:
                    assign_pare1 = a
                    break
            
            for a in pare2:
                if a.torn_id == necessitat.servei and a.data == necessitat.data:
                    assign_pare2 = a
                    break
            
            # Filtrem assignacions que violarien la restricció d'una per dia
            candidats = []
            
            if assign_pare1:
                key = (assign_pare1.treballador_id, assign_pare1.data)
                if key not in treballadors_per_dia:
                    candidats.append(assign_pare1)
            
            if assign_pare2:
                key = (assign_pare2.treballador_id, assign_pare2.data)
                if key not in treballadors_per_dia:
                    candidats.append(assign_pare2)
            
            if not candidats:
                continue
            
            # Triem entre els candidats vàlids
            if len(candidats) == 1:
                assignacio_triada = candidats[0]
            else:
                # Avaluem quina és millor segons criteris d'equitat
                assign_pare1, assign_pare2 = candidats[0], candidats[1]
                treb1 = self.treballadors_grup_t.get(assign_pare1.treballador_id)
                treb2 = self.treballadors_grup_t.get(assign_pare2.treballador_id)
                
                score1 = 0
                score2 = 0
                
                if treb1:
                    if treb1.esta_dins_limit_estandard():
                        score1 += 2
                    if not assign_pare1.es_canvi_zona:
                        score1 += 1
                    if not assign_pare1.es_canvi_torn:
                        score1 += 1
                
                if treb2:
                    if treb2.esta_dins_limit_estandard():
                        score2 += 2
                    if not assign_pare2.es_canvi_zona:
                        score2 += 1
                    if not assign_pare2.es_canvi_torn:
                        score2 += 1
                
                # Selecció estocàstica basada en scores
                if score1 + score2 > 0:
                    prob_pare1 = score1 / (score1 + score2)
                    if random.random() < prob_pare1:
                        assignacio_triada = assign_pare1
                    else:
                        assignacio_triada = assign_pare2
                else:
                    assignacio_triada = random.choice([assign_pare1, assign_pare2])
            
            fill.append(assignacio_triada)
            treballadors_per_dia[(assignacio_triada.treballador_id, assignacio_triada.data)] = True
        
        return fill
    
    def mutacio(self, solucio: List[Assignacio], 
                prob_mutacio: float = 0.1) -> List[Assignacio]:
        """
        Mutació: canvia algunes assignacions prioritzant l'equitat
        VALIDACIÓ: Assegura que no es creïn duplicats de treballador-dia
        """
        nova_solucio = []
        # Control d'assignacions per treballador i dia
        treballadors_per_dia = {}
        
        # Primer passem per totes les assignacions per registrar-les
        for assign in solucio:
            treballadors_per_dia[(assign.treballador_id, assign.data)] = assign
        
        for assign in solucio:
            if random.random() < prob_mutacio:
                # Busquem la necessitat corresponent
                necessitat = None
                for nec in self.necessitats:
                    if nec.servei == assign.torn_id and nec.data == assign.data:
                        necessitat = nec
                        break
                
                if necessitat:
                    # Busquem treballadors alternatius del grup T
                    candidats = []
                    
                    for treb_id, treb in self.treballadors_grup_t.items():
                        # Skip el treballador actual
                        if treb_id == assign.treballador_id:
                            continue
                        
                        # VALIDACIÓ RÍGIDA: No pot tenir ja una assignació aquest dia
                        if (treb_id, necessitat.data) in treballadors_per_dia:
                            continue
                        
                        # Excloem segons exclude_map
                        if necessitat.data in self.exclude_map and treb_id in self.exclude_map[necessitat.data]:
                            continue

                        if treb.te_descans(necessitat.data):
                            continue
                        if treb.linia != necessitat.linia:
                            continue
                        if not necessitat.formacio.intersection(treb.habilitacions):
                            continue
                        
                        # Comprovem hores disponibles
                        if treb.hores_disponibles() < assign.durada_hores:
                            continue
                        
                        # VALIDACIÓ RÍGIDA: Ha de complir 12h de descans
                        if not self._compleix_descans_12h(treb_id, necessitat.data, assign.hora_inici, nova_solucio):
                            continue
                        
                        candidats.append(treb_id)
                    
                    if candidats:
                        # Triem un nou treballador
                        nou_treballador = random.choice(candidats)
                        treb = self.treballadors_grup_t[nou_treballador]
                        
                        # ACTUALITZEM el registre
                        treballadors_per_dia.pop((assign.treballador_id, assign.data), None)
                        
                        nova_assignacio = Assignacio(
                            treballador_id=nou_treballador,
                            torn_id=assign.torn_id,
                            data=assign.data,
                            hora_inici=assign.hora_inici,
                            hora_fi=assign.hora_fi,
                            durada_hores=assign.durada_hores,
                            es_canvi_zona=treb.es_canvi_zona(necessitat.zona),
                            es_canvi_torn=treb.es_canvi_torn(necessitat.torn)
                        )
                        
                        treballadors_per_dia[(nou_treballador, necessitat.data)] = nova_assignacio
                        nova_solucio.append(nova_assignacio)
                    else:
                        nova_solucio.append(assign)
                else:
                    nova_solucio.append(assign)
            else:
                nova_solucio.append(assign)
        
        return nova_solucio
    
    def evalua_validesa(self, solucio: List[Assignacio]) -> float:
        """
        Retorna una penalització basada en violacions de restriccions.
        Serveix per guiar l'AG cap a solucions més vàlides.
        Penalitzacions més altes = solucions pitjors.
        """
        penalitzacio = 0.0
        
        # Penalització per duplicats de treballador-dia
        treballador_dia = {}
        duplicats_treb_dia = 0
        for assign in solucio:
            key = (assign.treballador_id, assign.data)
            if key in treballador_dia:
                duplicats_treb_dia += 1
                penalitzacio += 50.0
            else:
                treballador_dia[key] = True
        
        # Penalització per duplicats de torn-data
        torn_data = {}
        duplicats_torn_data = 0
        for assign in solucio:
            key = (assign.torn_id, assign.data)
            if key in torn_data:
                duplicats_torn_data += 1
                penalitzacio += 50.0
            else:
                torn_data[key] = True
        
        # Penalització per necessitats descobertes
        necessitats_cobertes = set()
        for assign in solucio:
            for nec in self.necessitats:
                if nec.servei == assign.torn_id and nec.data == assign.data:
                    necessitats_cobertes.add((nec.servei, nec.data))
        
        necessitats_descobertes = len(self.necessitats) - len(necessitats_cobertes)
        penalitzacio += necessitats_descobertes * 20.0
        
        return penalitzacio
    
    def reparacio(self, solucio: List[Assignacio]) -> List[Assignacio]:
        """
        Repara una solució de manera intel·ligent:
        1. Elimina duplicats mantenint els millors
        2. Intenta reasignar les necessitats descobertes
        3. Aplica estratègia de reparació progressiva
        """
        # Pas 1: Identificar i resoldre duplicats de torn-data
        vistes_torn_data = {}  # {(torn_id, data): assignacio}
        treballador_dia_vistes = {}  # {(treballador_id, data): assignacio}
        solucio_sense_duplicats = []
        duplicats_eliminats = []
        
        for assign in solucio:
            key_torn = (assign.torn_id, assign.data)
            key_treb = (assign.treballador_id, assign.data)
            
            # Prioritat 1: Evitem duplicats de torn-data (crític)
            if key_torn in vistes_torn_data:
                duplicats_eliminats.append(assign)
                continue
            
            # Prioritat 2: Evitem duplicats de treballador-dia
            if key_treb in treballador_dia_vistes:
                duplicats_eliminats.append(assign)
                continue
            
            solucio_sense_duplicats.append(assign)
            vistes_torn_data[key_torn] = assign
            treballador_dia_vistes[key_treb] = assign
        
        # Pas 2: Intentem cobrir les necessitats descobertes per duplicats
        necessitats_descobertes = []
        for nec in self.necessitats:
            key = (nec.servei, nec.data)
            if key not in vistes_torn_data:
                necessitats_descobertes.append(nec)
        
        # Per cada necessitat descoberta, busquem un treballador lliure
        reasignacions_exitoses = 0
        for nec in necessitats_descobertes:
            # Cercle de búsqueda prioritzat: primer candidats que tenien la necessitat
            candidats_ordenats = []
            
            for treb_id, treb in self.treballadors_grup_t.items():
                key_treb = (treb_id, nec.data)
                
                # Saltem si ja té assignació aquest dia
                if key_treb in treballador_dia_vistes:
                    continue
                
                # Validacions bàsiques
                if treb.te_descans(nec.data):
                    continue
                if treb.linia != nec.linia:
                    continue
                if nec.formacio not in treb.habilitacions:
                    continue
                
                # Calculem prioritat: preferim treballadors que tenien aquesta necessitat
                prioritat = 0
                if not treb.es_canvi_zona(nec.zona):
                    prioritat += 10
                if not treb.es_canvi_torn(nec.torn):
                    prioritat += 10
                if treb.esta_dins_limit_estandard():
                    prioritat += 5
                
                candidats_ordenats.append((treb_id, prioritat))
            
            if not candidats_ordenats:
                continue
            
            # Ordenem per prioritat
            candidats_ordenats.sort(key=lambda x: x[1], reverse=True)
            
            # Intentem els millors candidats
            for treb_id, _ in candidats_ordenats:
                try:
                    servei = DataLoader.troba_servei_per_data(
                        self.torns[nec.servei], nec.data, self.calendari
                    )
                    
                    if not self._compleix_descans_12h(treb_id, nec.data, servei.hora_inici, solucio_sense_duplicats):
                        continue
                    
                    treb = self.treballadors_grup_t[treb_id]
                    
                    nova_assign = Assignacio(
                        treballador_id=treb_id,
                        torn_id=nec.servei,
                        data=nec.data,
                        hora_inici=servei.hora_inici,
                        hora_fi=servei.hora_fi,
                        durada_hores=servei.durada_hores(),
                        es_canvi_zona=treb.es_canvi_zona(nec.zona),
                        es_canvi_torn=treb.es_canvi_torn(nec.torn)
                    )
                    
                    solucio_sense_duplicats.append(nova_assign)
                    treballador_dia_vistes[key_treb] = nova_assign
                    vistes_torn_data[(nec.servei, nec.data)] = nova_assign
                    reasignacions_exitoses += 1
                    break  # Necessitat coberta, passem a la següent
                
                except:
                    continue
        
        return solucio_sense_duplicats
    
    def executa(self, generacions: int = 100, 
                verbose: bool = True) -> Tuple[List[Assignacio], Dict]:
        """
        Executa l'algorisme genètic amb reparació i evaluació de validesa integrades
        """
        poblacio = self.genera_poblacio_inicial()
        
        millor_global = max(poblacio, key=lambda x: x[1]['total'])
        
        if verbose:
            print(f"\n   Millor individu inicial: {millor_global[1]['total']:.2f}")
            print(f"   Assignacions inicials: {len(millor_global[0])}/{len(self.necessitats)}")
        
        generacions_sense_millora = 0
        
        for gen in range(generacions):
            nova_poblacio = []
            
            # Elitisme: mantenim els 3 millors
            poblacio_ordenada = sorted(poblacio, key=lambda x: x[1]['total'], reverse=True)
            nova_poblacio.extend(poblacio_ordenada[:3])
            
            # Generem la resta de la població
            while len(nova_poblacio) < self.mida_poblacio:
                pare1 = self.seleccio_torneig(poblacio)
                pare2 = self.seleccio_torneig(poblacio)
                
                fill = self.encreuament(pare1, pare2)
                
                # Mutació adaptativa
                prob_mut = 0.05 + (0.20 * generacions_sense_millora / 25)
                prob_mut = min(prob_mut, 0.35)
                fill = self.mutacio(fill, prob_mutacio=prob_mut)
                
                # NOVA LÍNA: Avaluem validesa antes de reparar
                validesa_penalty = self.evalua_validesa(fill)
                
                # NOVA LÍNA: Reparació intel·ligent si té problemes greus
                if validesa_penalty > 50:
                    fill = self.reparacio(fill)
                    validesa_penalty = self.evalua_validesa(fill)  # Reevaluem
                
                # Reparació sempre al final (passa de neteja)
                fill = self.reparacio(fill)
                
                resultat = self.restriccions.evalua_solucio(
                    fill, self.treballadors, self.torns, 
                    self.necessitats, self.calendari, self.estadistiques
                )
                
                # NOVA LÍNA: Integrem validesa en el score total
                resultat['validesa_penalty'] = validesa_penalty
                resultat['total'] -= validesa_penalty * 0.05  # Pes del 5%
                
                nova_poblacio.append((fill, resultat))
            
            poblacio = nova_poblacio
            millor_actual = max(poblacio, key=lambda x: x[1]['total'])
            
            if millor_actual[1]['total'] > millor_global[1]['total']:
                millor_global = millor_actual
                generacions_sense_millora = 0
            else:
                generacions_sense_millora += 1
            
            if verbose and gen % 10 == 0:
                validesa_global = self.evalua_validesa(millor_global[0])
                print(f"   Generació {gen:3d}: Millor = {millor_global[1]['total']:6.2f} | "
                      f"Actual = {millor_actual[1]['total']:6.2f} | "
                      f"Cobertes = {len(millor_global[0])}/{len(self.necessitats)} | "
                      f"Validesa = {validesa_global:6.1f} | "
                      f"Mut = {prob_mut:.2f}")
            
            # Reinici si portem molt temps sense millora
            if generacions_sense_millora > 35:
                if verbose:
                    print(f"   ↻ Reiniciant diversitat (gen {gen})...")
                
                nous_individus = []
                for _ in range(self.mida_poblacio - 5):
                    sol = self.genera_solucio_aleatoria()
                    sol = self.mutacio(sol, prob_mutacio=0.5)
                    sol = self.reparacio(sol)
                    res = self.restriccions.evalua_solucio(
                        sol, self.treballadors, self.torns,
                        self.necessitats, self.calendari, self.estadistiques
                    )
                    validesa_penalty = self.evalua_validesa(sol)
                    res['validesa_penalty'] = validesa_penalty
                    res['total'] -= validesa_penalty * 0.05
                    nous_individus.append((sol, res))
                
                poblacio = poblacio_ordenada[:5] + nous_individus
                generacions_sense_millora = 0
        
        if verbose:
            print(f"\n   ✓ Algorisme finalitzat!")
            validesa_final = self.evalua_validesa(millor_global[0])
            print(f"   → Millor score final: {millor_global[1]['total']:.2f}")
            print(f"   → Penalització validesa final: {validesa_final:.1f}")
            print(f"   → Assignacions finals: {len(millor_global[0])}/{len(self.necessitats)}")
        
        return millor_global[0], millor_global[1]