# data_loader.py - VERSIÓ SQLite

import sqlite3
import csv
import re
from typing import List, Dict, Tuple, Set, Optional
from datetime import time, date, datetime
from data_structures import (
    Torn, ServeiTorn, DiaCalendari, Treballador,
    NecessitatCobertura, HistoricTreballador, EstadistiquesGlobals
)


class DataLoader:
    def __init__(self, db_path: str = 'treballadors.db'):
        """
        Inicialitza el DataLoader amb la ruta de la base de dades SQLite
        
        Args:
            db_path: Ruta al fitxer de la base de dades SQLite
        """
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None
    
    def connect(self) -> bool:
        """Connecta a la base de dades SQLite"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            return True
        except sqlite3.Error as e:
            print(f"✗ Error de connexió a la base de dades: {e}")
            return False
    
    def close(self) -> None:
        """Tanca la connexió a la base de dades"""
        if self.conn:
            self.conn.close()
    
    @staticmethod
    def parse_time(time_str: str) -> time:
        """Converteix string d'hora a objecte time"""
        if time_str is None:
            raise ValueError("Hora buida")
        s = str(time_str).replace('"', '').strip()
        if not s:
            raise ValueError("Hora buida")
        # Format acceptats: H:M, HH:MM, HMM, HHMM, H or HH
        try:
            if ':' in s:
                parts = s.split(':')
                if len(parts) != 2:
                    raise ValueError(f"Format d'hora invàlid: {time_str}")
                hours = int(parts[0])
                minutes = int(parts[1])
            else:
                digits = ''.join(ch for ch in s if ch.isdigit())
                if not digits:
                    raise ValueError(f"Format d'hora invàlid: {time_str}")
                if len(digits) <= 2:
                    hours = int(digits)
                    minutes = 0
                elif len(digits) == 3:
                    hours = int(digits[0])
                    minutes = int(digits[1:])
                else:
                    hours = int(digits[:-2])
                    minutes = int(digits[-2:])
            return time(hour=hours % 24, minute=minutes)
        except Exception as e:
            raise ValueError(f"No s'ha pogut parsejar l'hora '{time_str}': {e}") from e
    
    @staticmethod
    def parse_date(date_str: str) -> date:
        """Converteix string de data a objecte date"""
        return datetime.strptime(date_str, '%d/%m/%Y').date()
    
    @staticmethod
    def parse_date_flexible(date_str: str) -> date:
        """Converteix string de data a objecte date amb formats flexibles"""
        formats = ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d']
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        raise ValueError(f"Format de data no reconegut: {date_str}")
    
    def carrega_torns(self) -> Dict[str, Torn]:
        """
        Carrega els torns amb els seus serveis des de la taula serveis_horaris
        """
        if self.cursor is None or self.conn is None:
            if not self.connect():
                raise RuntimeError("No s'ha pogut connectar a la base de dades")
        
        torns = {}
        
        query = 'SELECT * FROM serveis_horaris'
        self.cursor.execute(query)
        
        columns = [description[0] for description in self.cursor.description]
        
        for row in self.cursor.fetchall():
            row_dict = dict(zip(columns, row))
            
            torn_id = row_dict['Torn']
            linia = row_dict.get('Línia', '')
            zona = row_dict.get('Zona', '')
            
            serveis = {}
            
            # Processem cada servei (S1, S2, S3, S4)
            for i in range(1, 5):
                servei_col = f'Servei {i}'
                inici_col = f'Inici S{i}'
                final_col = f'Final S{i}'
                
                if (row_dict.get(servei_col) and 
                    row_dict.get(inici_col) and 
                    row_dict.get(final_col)):
                    
                    codis_str = str(row_dict[servei_col]).replace('"', '').strip()
                    if not codis_str:
                        continue
                    
                    codis = set(codis_str.split(','))
                    hora_inici = self.parse_time(row_dict[inici_col])
                    hora_fi = self.parse_time(row_dict[final_col])
                    creua_mitjanit = hora_fi < hora_inici
                    
                    servei = ServeiTorn(
                        num_servei=i,
                        codis_servei=codis,
                        hora_inici=hora_inici,
                        hora_fi=hora_fi,
                        creua_mitjanit=creua_mitjanit
                    )
                    serveis[i] = servei
            
            torn = Torn(
                id=torn_id,
                linia=linia,
                zona=zona,
                serveis=serveis
            )
            torns[torn_id] = torn
        
        return torns
    
    def carrega_calendari(self) -> Dict[date, DiaCalendari]:
        """
        Carrega el calendari de serveis des de la taula serveis_calendari
        """
        if self.cursor is None or self.conn is None:
            if not self.connect():
                raise RuntimeError("No s'ha pogut connectar a la base de dades")
        
        calendari = {}
        
        query = 'SELECT * FROM serveis_calendari'
        self.cursor.execute(query)
        
        columns = [description[0] for description in self.cursor.description]
        
        for row in self.cursor.fetchall():
            row_dict = dict(zip(columns, row))
            
            # Parsejar la data
            data = self.parse_date(row_dict['Data'])
            
            dia = DiaCalendari(
                data=data,
                servei_bv=row_dict.get('Servei BV', '').strip(),
                dia_setmana=row_dict.get('Dia_Set', '').strip(),
                dia_mes=row_dict.get('Dia_Mes', '').strip(),
                dia_num=row_dict.get('Dia_Num', '').strip()
            )
            calendari[data] = dia
        
        return calendari
    
    @staticmethod
    def troba_servei_per_data(torn: Torn, data: date,
                               calendari: Dict[date, DiaCalendari]) -> ServeiTorn:
        """
        Troba quin servei (S1/S2/S3/S4) del torn aplica per una data concreta
        """
        if data not in calendari:
            raise ValueError(f"Data {data} no trobada al calendari")
        
        codi_dia = calendari[data].servei_bv
        
        for num_servei, servei in torn.serveis.items():
            if codi_dia in servei.codis_servei:
                return servei
        
        raise ValueError(f"Codi servei {codi_dia} no trobat al torn {torn.id}")
    
    def carrega_descansos_dies(self) -> Dict[str, Set[date]]:
        """
        Carrega els descansos dels treballadors des de la taula descansos_dies
        Retorna: Dict[treballador_id, Set[date]]
        """
        if self.cursor is None or self.conn is None:
            if not self.connect():
                raise RuntimeError("No s'ha pogut connectar a la base de dades")
        
        descansos_treballadors = {}
        
        try:
            query = 'SELECT treballador_id, data FROM descansos_dies'
            self.cursor.execute(query)
            
            for row in self.cursor.fetchall():
                treballador_id = str(row[0])
                data_str = row[1]
                
                # Parsejem la data amb format flexible
                data = self.parse_date_flexible(data_str)
                
                # Inicialitzem el set per aquest treballador si no existeix
                if treballador_id not in descansos_treballadors:
                    descansos_treballadors[treballador_id] = set()
                
                # Afegim la data de descans directament
                descansos_treballadors[treballador_id].add(data)
            
            print(f" ✓ Descansos carregats per {len(descansos_treballadors)} treballadors")
        
        except sqlite3.Error as e:
            print(f" ⚠️ Error carregant descansos: {e}")
        
        return descansos_treballadors
    
    def carrega_treballadors(self) -> Dict[str, Treballador]:
        """
        Carrega els treballadors disponibles des de la taula treballadors
        Els descansos es carreguen des de la taula descansos_dies
        """
        if self.cursor is None or self.conn is None:
            if not self.connect():
                raise RuntimeError("No s'ha pogut connectar a la base de dades")
        
        # Primer carreguem els descansos
        descansos_treballadors = self.carrega_descansos_dies()
        
        treballadors = {}
        
        query = 'SELECT * FROM treballadors'
        self.cursor.execute(query)
        
        columns = [description[0] for description in self.cursor.description]
        
        for row in self.cursor.fetchall():
            row_dict = dict(zip(columns, row))
            
            treballador_id = str(row_dict['id'])
            
            # Obtenim els descansos d'aquest treballador
            dates_descans = descansos_treballadors.get(treballador_id, set())
            
            # Processem habilitacions
            habilitacions_str = str(row_dict.get('habilitacions', '')).strip()
            habilitacions = set()
            if habilitacions_str:
                habilitacions = set(h.strip() for h in habilitacions_str.replace('+', ',').split(',') if h.strip())
            
            treballador = Treballador(
                id=treballador_id,
                nom=row_dict['treballador'],
                plaza=row_dict.get('plaza', ''),
                torn_assignat=row_dict.get('rotacio', row_dict.get('torn', '')),
                zona=row_dict.get('zona', ''),
                habilitacions=habilitacions,
                linia=row_dict.get('línia', ''),
                categoria=row_dict.get('categoria', ''),
                grup=row_dict.get('grup', ''),
                denominacio=row_dict.get('denominació', ''),
                dates_descans=dates_descans,
                hores_anuals_realitzades=0.0,
                max_hores_anuals=1218.0,
                max_hores_ampliables=1605.0,
                canvis_zona=0,
                canvis_torn=0
            )
            treballadors[treballador_id] = treballador
        
        return treballadors
    
    def carrega_necessitats_cobertura(self) -> List[NecessitatCobertura]:
        """
        Carrega els torns que necessiten cobertura des de la taula cobertura
        """
        if self.cursor is None or self.conn is None:
            if not self.connect():
                raise RuntimeError("No s'ha pogut connectar a la base de dades")
        
        necessitats = []
        
        query = 'SELECT * FROM cobertura'
        self.cursor.execute(query)
        
        columns = [description[0] for description in self.cursor.description]

        for row in self.cursor.fetchall():
            row_dict = dict(zip(columns, row))

            data = datetime.strptime(row_dict['data'], '%Y-%m-%d').date()

            torn_val = row_dict.get('rotacio', row_dict.get('torn', ''))

            # ⭐ AFEGEIX AQUEST BLOC NOU ⭐
            formacio_str = str(row_dict.get('formacio', '')).strip()
            formacions_set = set()
            if formacio_str:
                formacions_set = set(f.strip() for f in formacio_str.replace('+', ',').split(',') if f.strip())

            necessitat = NecessitatCobertura(
                servei=row_dict.get('servei', ''),
                residencia=row_dict.get('residencia', ''),
                torn=torn_val,
                formacio=formacions_set,  # ⭐ CANVIAT ⭐
                linia=row_dict.get('linia', ''),
                zona=row_dict.get('zona', ''),
                motiu=row_dict.get('motiu_no_cobert', ''),
                data=data
            )
        
            necessitats.append(necessitat)
        
        return necessitats
    
    def carrega_historic(self, treballadors: Dict[str, Treballador]) -> EstadistiquesGlobals:
        """
        Carrega l'històric d'assignacions prèvies des de la taula historic_assignacions
        """
        if self.cursor is None or self.conn is None:
            if not self.connect():
                raise RuntimeError("No s'ha pogut connectar a la base de dades")
        
        from data_structures import Assignacio
        
        estadistiques = EstadistiquesGlobals()
        
        try:
            query = 'SELECT * FROM historic_assignacions'
            self.cursor.execute(query)
            
            columns = [description[0] for description in self.cursor.description]
            
            for row in self.cursor.fetchall():
                row_dict = dict(zip(columns, row))
                
                treb_id = str(row_dict['treballador_id'])
                
                if treb_id not in treballadors:
                    continue
                
                data = datetime.strptime(row_dict['data'], '%Y-%m-%d').date()
                hora_inici = datetime.strptime(row_dict['hora_inici'], '%H:%M').time()
                hora_fi = datetime.strptime(row_dict['hora_fi'], '%H:%M').time()
                
                apunt_data = None
                if row_dict.get('data_apunt'):
                    try:
                        apunt_data = datetime.fromisoformat(row_dict['data_apunt'].strip())
                    except Exception:
                        try:
                            apunt_data = datetime.strptime(row_dict['data_apunt'].strip(), '%Y-%m-%d %H:%M:%S')
                        except Exception:
                            apunt_data = None
                
                # Convertir valors booleans
                es_canvi_zona = bool(row_dict.get('es_canvi_zona', 0))
                es_canvi_torn = bool(row_dict.get('es_canvi_torn', 0))
                
                assignacio = Assignacio(
                    treballador_id=treb_id,
                    torn_id=row_dict['torn_id'],
                    data=data,
                    hora_inici=hora_inici,
                    hora_fi=hora_fi,
                    durada_hores=float(row_dict['durada_hores']),
                    es_canvi_zona=es_canvi_zona,
                    es_canvi_torn=es_canvi_torn,
                    apunt_data=apunt_data
                )
                
                historic = estadistiques.get_historic(treb_id)
                historic.afegir_assignacio(assignacio)
                
                treballadors[treb_id].hores_anuals_realitzades += assignacio.durada_hores
                if assignacio.es_canvi_zona:
                    treballadors[treb_id].canvis_zona += 1
                if assignacio.es_canvi_torn:
                    treballadors[treb_id].canvis_torn += 1
            
            print(f" ✓ Històric carregat: {len(estadistiques.historials)} treballadors")
        
        except sqlite3.Error as e:
            print(f" ⚠️ Error carregant històric: {e}")
        
        return estadistiques
    
    # Afegir aquests mètodes a la classe DataLoader en data_loader.py

    def reinicia_taula_assig_grup_T(self) -> bool:
        """
        Esborra tot el contingut de la taula assig_grup_T
        Això es fa SEMPRE al començar una nova execució
        """
        if self.cursor is None or self.conn is None:
            if not self.connect():
                raise RuntimeError("No s'ha pogut connectar a la base de dades")
        
        try:
            self.cursor.execute('DELETE FROM assig_grup_T')
            self.conn.commit()
            print(" ✓ Taula assig_grup_T reiniciada (contingut esborrat)")
            return True
        except sqlite3.Error as e:
            print(f" ✗ Error reiniciant taula assig_grup_T: {e}")
            return False


    def guarda_assignacions_grup_T(self, assignacions: list, treballadors: Dict[str, 'Treballador'], 
                                    calendari: Dict[date, 'DiaCalendari'], 
                                    necessitats: list) -> bool:
        """
        Guarda les assignacions trobades per l'algorisme a la taula assig_grup_T
        
        Args:
            assignacions: Llista d'objectes Assignacio
            treballadors: Diccionari de treballadors
            calendari: Diccionari del calendari
            necessitats: Llista de necessitats (per obtenir info addicional)
        
        Returns:
            True si s'ha guardat correctament, False altrament
        """
        if self.cursor is None or self.conn is None:
            if not self.connect():
                raise RuntimeError("No s'ha pogut connectar a la base de dades")
        
        try:
            insert_query = """
                INSERT INTO assig_grup_T 
                (data, dia_setmana, torn, treballador_id, treballador_nom, 
                treballador_plaza, treballador_grup, hora_inici, hora_fi, 
                durada_hores, linia, zona, formacio, es_canvi_zona, 
                es_canvi_torn, hores_totals_any)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Crear un mapa de necessitats per accés ràpid
            necessitats_map = {}
            for nec in necessitats:
                key = (nec.servei, nec.data)
                necessitats_map[key] = nec
            
            registres_insertats = 0
            
            for assign in assignacions:
                treballador = treballadors[assign.treballador_id]
                
                # Obtenir dia_setmana del calendari
                dia_setmana = ''
                if assign.data in calendari:
                    dia_setmana = calendari[assign.data].dia_setmana
                
                # Buscar la necessitat corresponent per obtenir info addicional
                necessitat = necessitats_map.get((assign.torn_id, assign.data))
                
                linia = necessitat.linia if necessitat else ''
                zona = necessitat.zona if necessitat else ''
                formacio = necessitat.formacio if necessitat else ''
                
                self.cursor.execute(insert_query, (
                    assign.data.strftime('%Y-%m-%d'),
                    dia_setmana,
                    assign.torn_id,
                    assign.treballador_id,
                    treballador.nom,
                    treballador.plaza,
                    treballador.grup,
                    assign.hora_inici.strftime('%H:%M'),
                    assign.hora_fi.strftime('%H:%M'),
                    float(assign.durada_hores),
                    linia,
                    zona,
                    formacio,
                    int(assign.es_canvi_zona),
                    int(assign.es_canvi_torn),
                    float(treballador.hores_anuals_realitzades)
                ))
                registres_insertats += 1
            
            self.conn.commit()
            print(f" ✓ {registres_insertats} assignacions guardades a assig_grup_T")
            return True
            
        except sqlite3.Error as e:
            print(f" ✗ Error guardant assignacions a assig_grup_T: {e}")
            if self.conn:
                self.conn.rollback()
            return False
        except Exception as e:
            print(f" ✗ Error inesperat guardant assignacions: {e}")
            if self.conn:
                self.conn.rollback()
            return False


    def compta_registres_assig_grup_T(self) -> int:
        """
        Compta el nombre de registres actuals a assig_grup_T
        """
        if self.cursor is None or self.conn is None:
            if not self.connect():
                return 0
        
        try:
            self.cursor.execute('SELECT COUNT(*) FROM assig_grup_T')
            count = self.cursor.fetchone()[0]
            return count
        except sqlite3.Error:
            return 0
    
    def guarda_historic(self, estadistiques: EstadistiquesGlobals, 
                       csv_path: str = 'historic_assignacions.csv') -> None:
        """
        Guarda l'històric actualitzat d'assignacions tant a SQLite com a CSV
        
        Args:
            estadistiques: Objecte amb les estadístiques globals
            csv_path: Ruta del fitxer CSV de backup (opcional)
        """
        if self.cursor is None or self.conn is None:
            if not self.connect():
                raise RuntimeError("No s'ha pogut connectar a la base de dades")
        
        try:
            # 1. Esborrar dades antigues de la taula
            self.cursor.execute('DELETE FROM historic_assignacions')
            
            # 2. Insertar noves assignacions a SQLite
            insert_query = '''
                INSERT INTO historic_assignacions 
                (treballador_id, torn_id, data, hora_inici, hora_fi, 
                 durada_hores, es_canvi_zona, es_canvi_torn, data_apunt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            for historic in estadistiques.historials.values():
                for assignacio in historic.assignacions_any:
                    data_apunt = (assignacio.apunt_data.isoformat() 
                                 if getattr(assignacio, 'apunt_data', None) 
                                 else datetime.now().isoformat())
                    
                    self.cursor.execute(insert_query, (
                        assignacio.treballador_id,
                        assignacio.torn_id,
                        assignacio.data.strftime('%Y-%m-%d'),
                        assignacio.hora_inici.strftime('%H:%M'),
                        assignacio.hora_fi.strftime('%H:%M'),
                        float(assignacio.durada_hores),
                        int(assignacio.es_canvi_zona),
                        int(assignacio.es_canvi_torn),
                        data_apunt
                    ))
            
            self.conn.commit()
            print(f" ✓ Històric guardat a SQLite ({len(estadistiques.historials)} treballadors)")
            
            # 3. També guardar a CSV (backup)
            with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['treballador_id', 'torn_id', 'data', 'hora_inici',
                               'hora_fi', 'durada_hores', 'es_canvi_zona', 'es_canvi_torn', 'data_apunt'])
                
                for historic in estadistiques.historials.values():
                    for assignacio in historic.assignacions_any:
                        data_apunt = (assignacio.apunt_data.isoformat() 
                                     if getattr(assignacio, 'apunt_data', None) 
                                     else datetime.now().isoformat())
                        
                        writer.writerow([
                            assignacio.treballador_id,
                            assignacio.torn_id,
                            assignacio.data.strftime('%Y-%m-%d'),
                            assignacio.hora_inici.strftime('%H:%M'),
                            assignacio.hora_fi.strftime('%H:%M'),
                            f"{assignacio.durada_hores:.2f}",
                            assignacio.es_canvi_zona,
                            assignacio.es_canvi_torn,
                            data_apunt
                        ])
            
            print(f" ✓ Històric guardat també a CSV: {csv_path}")
        
        except sqlite3.Error as e:
            print(f" ✗ Error guardant històric a SQLite: {e}")
            if self.conn:
                self.conn.rollback()
        except Exception as e:
            print(f" ✗ Error guardant històric a CSV: {e}")
