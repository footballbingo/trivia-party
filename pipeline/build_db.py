#!/usr/bin/env python3
"""
Trivia Party — Football Bingo database pipeline.

Pulls notable players for a set of clubs from Wikidata (careers with dates,
nationality, position), derives trophy tags from curated winners lists
(UCL by club+year; WC/Euro/Copa America by country+year x tournament
participation), and writes ../bingo-data.js for the game to load.

Rerun anytime:  python3 build_db.py            (uses cached raw data if present)
                python3 build_db.py --fresh    (refetches everything)
"""
import json, os, re, sys, time, urllib.request, urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
UA = {"User-Agent": "TriviaPartyBot/0.1 (prototype data pipeline; contact 5of5juices@gmail.com)"}
SPARQL = "https://query.wikidata.org/sparql"
MIN_SITELINKS = 28          # notability floor for the pool
CASUAL_SITELINKS = 68       # superstars tier
EXPERT_SITELINKS = 40       # expert tier floor (full >=28 pool stays cached for later)
MIN_BIRTH = 1955            # keep it modern-ish era

def q(query, tries=5):
    data = urllib.parse.urlencode({"query": query}).encode()
    for i in range(tries):
        try:
            req = urllib.request.Request(SPARQL, data=data,
                headers={**UA, "Accept": "application/sparql-results+json",
                         "Content-Type": "application/x-www-form-urlencoded"})
            return json.load(urllib.request.urlopen(req, timeout=90))["results"]["bindings"]
        except Exception as ex:
            wait = 10 * (i + 1)
            print(f"    query retry {i+1} ({str(ex)[:80]}) — waiting {wait}s", flush=True)
            time.sleep(wait)
    raise RuntimeError("SPARQL failed after retries")

def val(row, key, default=None):
    return row.get(key, {}).get("value", default)

# ---------------------------------------------------------------- stage 1
def fetch_pool(clubs):
    """players with >= MIN_SITELINKS wikipedia sitelinks who played for our clubs"""
    pool = {}
    for cid, c in clubs.items():
        rows = q(f"""
SELECT DISTINCT ?p ?pLabel ?links WHERE {{
  ?p p:P54/ps:P54 wd:{c['qid']} ; wikibase:sitelinks ?links ; wdt:P569 ?dob .
  FILTER(?links >= {MIN_SITELINKS})
  FILTER(YEAR(?dob) >= {MIN_BIRTH})
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}""")
        for r in rows:
            qid = val(r, "p").rsplit("/", 1)[-1]
            pool[qid] = {"name": val(r, "pLabel"), "links": int(val(r, "links"))}
        print(f"  {cid:12} +{len(rows):4}  (pool: {len(pool)})", flush=True)
        time.sleep(1.2)
    return pool

# ---------------------------------------------------------------- stage 2
def fetch_careers(qids):
    """all club-team memberships with dates + whether team is a senior national team"""
    out = {}
    for i in range(0, len(qids), 60):
        chunk = qids[i:i+60]
        rows = q(f"""
SELECT ?p ?team ?teamLabel ?start ?end ?nat WHERE {{
  VALUES ?p {{ {' '.join('wd:'+x for x in chunk)} }}
  ?p p:P54 ?st . ?st ps:P54 ?team .
  OPTIONAL {{ ?st pq:P580 ?start . }}
  OPTIONAL {{ ?st pq:P582 ?end . }}
  BIND(EXISTS {{ ?team wdt:P31 wd:Q6979593 }} AS ?nat)
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}""")
        for r in rows:
            pid = val(r, "p").rsplit("/", 1)[-1]
            out.setdefault(pid, []).append({
                "team": val(r, "team").rsplit("/", 1)[-1],
                "teamLabel": val(r, "teamLabel"),
                "start": (val(r, "start") or "")[:10],
                "end": (val(r, "end") or "")[:10],
                "national": val(r, "nat") == "true",
            })
        print(f"  careers {i+len(chunk)}/{len(qids)}", flush=True)
        time.sleep(1.5)
    return out

def fetch_scalars(qids):
    """position(s), country-for-sport, citizenship, awards, tournament participations"""
    out = {}
    for i in range(0, len(qids), 50):
        chunk = qids[i:i+50]
        rows = q(f"""
SELECT ?p
  (GROUP_CONCAT(DISTINCT ?posQ;separator="|") AS ?pos)
  (GROUP_CONCAT(DISTINCT ?csL;separator="|") AS ?cs)
  (GROUP_CONCAT(DISTINCT ?citL;separator="|") AS ?cit)
  (GROUP_CONCAT(DISTINCT ?awQ;separator="|") AS ?aw)
  (GROUP_CONCAT(DISTINCT ?partL;separator="|") AS ?part)
WHERE {{
  VALUES ?p {{ {' '.join('wd:'+x for x in chunk)} }}
  OPTIONAL {{ ?p wdt:P413 ?posQ_ . BIND(STRAFTER(STR(?posQ_),"entity/") AS ?posQ) }}
  OPTIONAL {{ ?p wdt:P1532 ?cs_ . ?cs_ rdfs:label ?csL . FILTER(LANG(?csL)="en") }}
  OPTIONAL {{ ?p wdt:P27 ?cit_ . ?cit_ rdfs:label ?citL . FILTER(LANG(?citL)="en") }}
  OPTIONAL {{ ?p wdt:P166 ?aw_ . BIND(STRAFTER(STR(?aw_),"entity/") AS ?awQ) }}
  OPTIONAL {{ ?p wdt:P1344 ?part_ . ?part_ rdfs:label ?partL . FILTER(LANG(?partL)="en") }}
}} GROUP BY ?p""")
        for r in rows:
            pid = val(r, "p").rsplit("/", 1)[-1]
            out[pid] = {k: [x for x in (val(r, k) or "").split("|") if x]
                        for k in ("pos", "cs", "cit", "aw", "part")}
        print(f"  scalars {i+len(chunk)}/{len(qids)}", flush=True)
        time.sleep(1.5)
    return out

# ---------------------------------------------------------------- curated facts
POS_MAP = {"Q201330": "gk", "Q336286": "df", "Q193592": "mf", "Q280658": "fw",
           "Q1875713": "mf",  # attacking midfielder
           "Q10995371": "df"}
BALLON_QIDS = {"Q166177", "Q1361348"}  # Ballon d'Or, FIFA Ballon d'Or

COUNTRY_CODE = {
 "Argentina":"arg","Brazil":"bra","Uruguay":"uru","Colombia":"col","Chile":"chi",
 "France":"fra","Germany":"ger","Spain":"esp","Italy":"ita","Portugal":"por",
 "Netherlands":"ned","England":"eng","Belgium":"bel","Croatia":"cro","Switzerland":"sui",
 "Denmark":"den","Sweden":"swe","Poland":"pol","Serbia":"srb","Scotland":"sco",
 "Wales":"wal","Ireland":"irl","Republic of Ireland":"irl","Austria":"aut","Turkey":"tur","Türkiye":"tur",
 "Senegal":"sen","Ivory Coast":"civ","Côte d'Ivoire":"civ","Nigeria":"nga","Ghana":"gha",
 "Cameroon":"cmr","Morocco":"mar","Algeria":"alg","Egypt":"egy","Mexico":"mex",
 "United States":"usa","United States of America":"usa","Japan":"jpn","South Korea":"kor",
 "Norway":"nor","Czech Republic":"cze","Czechia":"cze","Ukraine":"ukr","Russia":"rus",
 "Greece":"gre","Romania":"rou","Slovakia":"svk","Hungary":"hun","Bosnia and Herzegovina":"bih",
 "Slovenia":"svn","Ecuador":"ecu","Paraguay":"par","Peru":"per","Canada":"can","Australia":"aus",
}
NAT_TEAM_RE = re.compile(r"^(.*?) (men's )?national (association football|football|soccer) team$", re.I)
SAMERICA = {"arg","bra","uru","col","chi","ecu","par","per"}
AFRICA = {"sen","civ","nga","gha","cmr","mar","alg","egy"}

# UCL / European Cup winners (year of final -> our club id), clubs in our list only
UCL_WINNERS = {
 1981:"pool",1984:"pool",1985:"juve",1987:"porto",1988:"psv",1989:"milan",1990:"milan",
 1992:"barca",1993:"marseille",1994:"milan",1995:"ajax",1996:"juve",1997:"dortmund",
 1998:"real",1999:"utd",2000:"real",2001:"bayern",2002:"real",2003:"milan",2004:"porto",
 2005:"pool",2006:"barca",2007:"milan",2008:"utd",2009:"barca",2010:"inter",2011:"barca",
 2012:"chelsea",2013:"bayern",2014:"real",2015:"barca",2016:"real",2017:"real",2018:"real",
 2019:"pool",2020:"bayern",2021:"chelsea",2022:"real",2023:"city",2024:"real",2025:"psg",
}
WC_WINNERS = {1986:"arg",1990:"ger",1994:"bra",1998:"fra",2002:"bra",2006:"ita",
              2010:"esp",2014:"ger",2018:"fra",2022:"arg"}
EURO_WINNERS = {1984:"fra",1988:"ned",1992:"den",1996:"ger",2000:"fra",2004:"gre",
                2008:"esp",2012:"esp",2016:"por",2020:"ita",2024:"esp"}
COPA_WINNERS = {1991:"arg",1993:"arg",1995:"uru",1997:"bra",1999:"bra",2001:"col",
                2004:"bra",2007:"bra",2011:"uru",2015:"chi",2016:"chi",2019:"bra",
                2021:"arg",2024:"arg"}

def overlaps(spell, year, cutoff_month_day="05-31"):
    """was the player at the club on ~final day of that season-year?"""
    final = f"{year}-{cutoff_month_day}"
    s, e = spell["start"], spell["end"]
    if not s: return False                      # dateless spells: too risky, skip
    return s <= final and (not e or e >= final)

def tourn_year(label, kind):
    m = re.search(r"(19|20)\d\d", label or "")
    if not m: return None
    if kind == "wc" and "world cup" not in label.lower(): return None
    if kind == "euro" and "euro" not in label.lower(): return None
    if kind == "copa" and "copa am" not in label.lower(): return None
    return int(m.group(0))


# additive insurance for famous trophy facts (only applied if the name is in the pool)
TOPUP = {}
def _t(tag, names):
    for n in names: TOPUP.setdefault(n, set()).add(tag)
_t("ballon", ["Lothar Matthäus","Jean-Pierre Papin","Marco van Basten","Roberto Baggio","Hristo Stoichkov",
  "George Weah","Matthias Sammer","Ronaldo","Ronaldo Nazário","Zinedine Zidane","Rivaldo","Luís Figo",
  "Michael Owen","Pavel Nedvěd","Andriy Shevchenko","Ronaldinho","Fabio Cannavaro","Kaká",
  "Cristiano Ronaldo","Lionel Messi","Luka Modrić","Karim Benzema","Rodri","Ousmane Dembélé"])
_t("copawin", ["Lionel Messi","Ángel Di María","Emiliano Martínez","Rodrigo De Paul","Nicolás Otamendi",
  "Cristian Romero","Lautaro Martínez","Leandro Paredes","Giovani Lo Celso","Julián Álvarez","Enzo Fernández",
  "Alexis Mac Allister","Nahuel Molina","Alexis Sánchez","Arturo Vidal","Claudio Bravo","Gary Medel",
  "Eduardo Vargas","Charles Aránguiz","Thiago Silva","Dani Alves","Casemiro","Gabriel Jesus","Roberto Firmino",
  "Marquinhos","Alisson","Alisson Becker","Luis Suárez","Edinson Cavani","Diego Forlán","Diego Godín"])
_t("eurowin", ["Cristiano Ronaldo","Pepe","Nani","Rui Patrício","João Moutinho","José Fonte","Renato Sanches",
  "Gianluigi Donnarumma","Leonardo Bonucci","Giorgio Chiellini","Lorenzo Insigne","Ciro Immobile",
  "Marco Verratti","Federico Chiesa","Jorginho","Nicolò Barella","Xavi","Andrés Iniesta","Iker Casillas",
  "Sergio Ramos","David Villa","Fernando Torres","Cesc Fàbregas","David Silva","Sergio Busquets",
  "Gerard Piqué","Xabi Alonso","Zinedine Zidane","Thierry Henry","Didier Deschamps","Lilian Thuram",
  "Patrick Vieira","Fabien Barthez","Jürgen Klinsmann","Rodri","Lamine Yamal","Nico Williams","Dani Olmo",
  "Álvaro Morata","Dani Carvajal","Fabián Ruiz","Mikel Merino","Unai Simón"])
_t("wcwin", ["Kylian Mbappé","Antoine Griezmann","Paul Pogba","N'Golo Kanté","Raphaël Varane","Hugo Lloris",
  "Olivier Giroud","Lionel Messi","Ángel Di María","Emiliano Martínez","Julián Álvarez","Enzo Fernández",
  "Manuel Neuer","Thomas Müller","Toni Kroos","Mesut Özil","Mats Hummels","Jérôme Boateng",
  "Bastian Schweinsteiger","Miroslav Klose","Mario Götze","Xavi","Andrés Iniesta","Iker Casillas",
  "Sergio Ramos","David Villa","Fernando Torres","Gerard Piqué","Sergio Busquets","Xabi Alonso","Carles Puyol",
  "Gianluigi Buffon","Fabio Cannavaro","Andrea Pirlo","Gennaro Gattuso","Francesco Totti","Alessandro Del Piero",
  "Marco Materazzi","Luca Toni","Gianluca Zambrotta","Fabio Grosso","Ronaldo","Ronaldo Nazário","Rivaldo",
  "Ronaldinho","Cafu","Roberto Carlos","Lúcio","Gilberto Silva","Zinedine Zidane","Thierry Henry",
  "Didier Deschamps","Lilian Thuram","Patrick Vieira","Fabien Barthez","Marcel Desailly","Emmanuel Petit",
  "Romário","Bebeto","Dunga","Cláudio Taffarel","Lothar Matthäus","Jürgen Klinsmann","Andreas Brehme",
  "Rudi Völler","Diego Maradona"])
TOPUP = {k: sorted(v) for k, v in TOPUP.items()}

# known false positives to strip (e.g. date-overlap ghosts: pre-contract signings etc.)
BLOCK = {"Kylian Mbappé": {"ucl", "ucl2"}}

# ---------------------------------------------------------------- assembly
def build():
    clubs = json.load(open(os.path.join(HERE, "clubs.json")))
    qid2cid = {c["qid"]: cid for cid, c in clubs.items()}
    cache = os.path.join(HERE, "raw_cache.json")

    if "--fresh" in sys.argv or not os.path.exists(cache):
        print("stage 1: player pool per club", flush=True)
        pool = fetch_pool(clubs)
        qids = list(pool.keys())
        print(f"pool: {len(qids)} players ≥{MIN_SITELINKS} sitelinks", flush=True)
        print("stage 2a: careers", flush=True)
        careers = fetch_careers(qids)
        print("stage 2b: scalars", flush=True)
        scalars = fetch_scalars(qids)
        json.dump({"pool": pool, "careers": careers, "scalars": scalars},
                  open(cache, "w"))
    else:
        d = json.load(open(cache)); pool, careers, scalars = d["pool"], d["careers"], d["scalars"]
        print(f"using cached raw data ({len(pool)} players)", flush=True)

    league_cc = {}   # our club id -> league country code (EN/ES/...)
    for cid, c in clubs.items(): league_cc[cid] = c["cc"]

    players = []
    for pid, meta in pool.items():
        spells = careers.get(pid, [])
        sc = scalars.get(pid, {"pos": [], "cs": [], "cit": [], "aw": [], "part": []})
        tags = set()

        # clubs + league countries
        club_spells = {}
        for sp in spells:
            cid = qid2cid.get(sp["team"])
            if cid:
                tags.add(cid)
                tags.add("lg_" + league_cc[cid].lower())
                club_spells.setdefault(cid, []).append(sp)

        # nationality: senior national team > country-for-sport > citizenship
        nat = None
        for sp in spells:
            if sp["national"]:
                m = NAT_TEAM_RE.match(sp["teamLabel"] or "")
                if m and COUNTRY_CODE.get(m.group(1)): nat = COUNTRY_CODE[m.group(1)]; break
        if not nat:
            for lbl in sc["cs"] + sc["cit"]:
                if COUNTRY_CODE.get(lbl): nat = COUNTRY_CODE[lbl]; break
        if nat:
            tags.add("nat_" + nat)
            if nat in SAMERICA: tags.add("samerica")
            if nat in AFRICA: tags.add("africa")

        # position
        for p in sc["pos"]:
            if p in POS_MAP: tags.add(POS_MAP[p])

        # Ballon d'Or
        if any(a in BALLON_QIDS for a in sc["aw"]): tags.add("ballon")

        # UCL titles from winners list x career dates
        ucl_years = {y for y, cid in UCL_WINNERS.items()
                     if cid in club_spells and any(overlaps(sp, y) for sp in club_spells[cid])}
        if ucl_years: tags.add("ucl")
        if len(ucl_years) >= 2: tags.add("ucl2")

        # international trophies via tournament participation x winners
        for lbl in sc["part"]:
            for kind, winners in (("wc", WC_WINNERS), ("euro", EURO_WINNERS), ("copa", COPA_WINNERS)):
                y = tourn_year(lbl, kind)
                if y and winners.get(y) == nat: tags.add(kind + "win")

        for extra in TOPUP.get(meta["name"], []): tags.add(extra)
        tags -= BLOCK.get(meta["name"], set())

        if len(tags & set(clubs.keys())) == 0: continue   # must have >=1 of our clubs
        players.append({"n": meta["name"], "links": meta["links"], "t": sorted(tags)})

    players.sort(key=lambda p: -p["links"])
    return clubs, players

# ---------------------------------------------------------------- emit JS
def emit(clubs, players):
    NATS = {}
    for p in players:
        for t in p["t"]:
            if t.startswith("nat_"): NATS[t] = NATS.get(t, 0) + 1
    keep_nats = {t for t, n in NATS.items() if n >= 6}

    FLAGCDN = {"arg":"ar","bra":"br","uru":"uy","col":"co","chi":"cl","fra":"fr","ger":"de",
      "esp":"es","ita":"it","por":"pt","ned":"nl","eng":"gb-eng","bel":"be","cro":"hr",
      "sui":"ch","den":"dk","swe":"se","pol":"pl","srb":"rs","sco":"gb-sct","wal":"gb-wls",
      "irl":"ie","aut":"at","tur":"tr","sen":"sn","civ":"ci","nga":"ng","gha":"gh","cmr":"cm",
      "mar":"ma","alg":"dz","egy":"eg","mex":"mx","usa":"us","jpn":"jp","kor":"kr","nor":"no",
      "cze":"cz","ukr":"ua","rus":"ru","gre":"gr","rou":"ro","svk":"sk","hun":"hu","bih":"ba",
      "svn":"si","ecu":"ec","par":"py","per":"pe","can":"ca","aus":"au"}
    NAT_LABEL = {"arg":"Argentina","bra":"Brazil","uru":"Uruguay","col":"Colombia","chi":"Chile",
      "fra":"France","ger":"Germany","esp":"Spain","ita":"Italy","por":"Portugal","ned":"Netherlands",
      "eng":"England","bel":"Belgium","cro":"Croatia","sui":"Switzerland","den":"Denmark","swe":"Sweden",
      "pol":"Poland","srb":"Serbia","sco":"Scotland","wal":"Wales","irl":"Ireland","aut":"Austria",
      "tur":"Türkiye","sen":"Senegal","civ":"Ivory Coast","nga":"Nigeria","gha":"Ghana","cmr":"Cameroon",
      "mar":"Morocco","alg":"Algeria","egy":"Egypt","mex":"Mexico","usa":"USA","jpn":"Japan","kor":"South Korea",
      "nor":"Norway","cze":"Czechia","ukr":"Ukraine","rus":"Russia","gre":"Greece","rou":"Romania",
      "svk":"Slovakia","hun":"Hungary","bih":"Bosnia","svn":"Slovenia","ecu":"Ecuador","par":"Paraguay",
      "per":"Peru","can":"Canada","aus":"Australia"}
    LG_LABEL = {"lg_en":"England League","lg_es":"Spain League","lg_it":"Italy League",
      "lg_de":"Germany League","lg_fr":"France League","lg_pt":"Portugal League",
      "lg_nl":"Netherlands Lg","lg_sc":"Scotland League","lg_tr":"Türkiye League","lg_ar":"Argentina Lg"}
    LG_FLAG = {"lg_en":"gb-eng","lg_es":"es","lg_it":"it","lg_de":"de","lg_fr":"fr",
      "lg_pt":"pt","lg_nl":"nl","lg_sc":"gb-sct","lg_tr":"tr","lg_ar":"ar"}
    CLUB_LABEL = {"real":"Real Madrid","barca":"Barcelona","atletico":"Atlético Madrid","sevilla":"Sevilla",
      "valencia":"Valencia","villarreal":"Villarreal","utd":"Man United","city":"Man City","pool":"Liverpool",
      "chelsea":"Chelsea","arsenal":"Arsenal","spurs":"Tottenham","everton":"Everton","newcastle":"Newcastle",
      "westham":"West Ham","leicester":"Leicester","milan":"AC Milan","inter":"Inter Milan","juve":"Juventus",
      "roma":"AS Roma","lazio":"Lazio","napoli":"Napoli","fio":"Fiorentina","bayern":"Bayern Munich",
      "dortmund":"Dortmund","leverkusen":"Leverkusen","schalke":"Schalke 04","psg":"PSG","marseille":"Marseille",
      "lyon":"Lyon","monaco":"Monaco","porto":"FC Porto","benfica":"Benfica","sporting":"Sporting CP",
      "ajax":"Ajax","psv":"PSV","celtic":"Celtic","rangers":"Rangers","galatasaray":"Galatasaray",
      "fenerbahce":"Fenerbahçe","boca":"Boca Juniors","river":"River Plate"}

    cond = {}
    def add(cid, label, img): cond[cid] = {"label": label, "img": img}
    for cid in clubs: add(cid, CLUB_LABEL.get(cid, cid), f"assets/badges/{cid}.png")
    for t in sorted(keep_nats):
        code = t[4:]; add(t, NAT_LABEL.get(code, code.upper()),
                          f"https://flagcdn.com/w320/{FLAGCDN.get(code,'un')}.png")
    for lg, lab in LG_LABEL.items(): add(lg, lab, f"https://flagcdn.com/w320/{LG_FLAG[lg]}.png")
    add("samerica", "South America", "assets/tiles/liberta.png")
    add("africa", "Africa", "assets/tiles/uefacup.png")
    add("gk", "Goalkeeper", "assets/tiles/gk.png"); add("df", "Defender", "assets/tiles/def.png")
    add("mf", "Midfielder", "assets/tiles/mid.png"); add("fw", "Forward", "assets/tiles/fwd.png")
    add("ucl", "Champions League", "assets/tiles/ucl.png"); add("ucl2", "2+ UCL Titles", "assets/tiles/ucl.png")
    add("ballon", "Ballon d'Or", "assets/tiles/ballon.png"); add("wcwin", "World Cup winner", "assets/tiles/wc.png")
    add("eurowin", "EURO winner", "assets/tiles/pltitle.png"); add("copawin", "Copa América winner", "assets/tiles/liberta.png")

    known = set(cond.keys())
    for p in players: p["t"] = [t for t in p["t"] if t in known]

    casual = [p for p in players if p["links"] >= CASUAL_SITELINKS]
    expert = [p for p in players if p["links"] >= EXPERT_SITELINKS]
    def js_players(rows):
        return ",\n".join('  {n:%s,t:%s}' % (json.dumps(r["n"]), json.dumps(r["t"])) for r in rows)

    out = f"""// GENERATED by pipeline/build_db.py — do not edit by hand. Rerun the pipeline instead.
// players: casual {len(casual)} / expert {len(expert)} · conditions: {len(cond)}
const BINGO_COND = {json.dumps(cond, ensure_ascii=False, indent=1)};
const BINGO_CASUAL = [
{js_players(casual)}
];
const BINGO_EXPERT = [
{js_players(expert)}
];
"""
    open(os.path.join(HERE, "..", "bingo-data.js"), "w").write(out)
    print(f"\nwrote bingo-data.js — casual {len(casual)} / expert {len(expert)} players, {len(cond)} conditions")
    # quick per-condition counts (expert pool)
    cnt = {}
    for p in expert:
        for t in p["t"]: cnt[t] = cnt.get(t, 0) + 1
    weak = {k: v for k, v in sorted(cnt.items(), key=lambda x: x[1]) if v < 4}
    print("conditions with <4 expert players (card builder will skip):", weak)

if __name__ == "__main__":
    clubs, players = build()
    emit(clubs, players)
