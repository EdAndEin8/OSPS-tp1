# OSPS – TP/Projet Python – Début du projet

**Auteurs :** Anton Dion, Arnaud Wissoqc – 4A FISA INFO, UPHF-INSA, 2026-2027

---

## 1. Lancement

Le projet utilise `uv` et Python 3.12 ou plus. Il ne doit pas être lancé en root : le programme le refuse.

```bash
uv sync                                   # installation (sysv-ipc)
uv run tp-1-anton-dion                    # terminal 1 : watchdog + serveurs
uv run tp-1-client requetetype1 "données" # terminal 2 : un client
uv run tp-1-test                          # terminal 2 : test fonctionnel
```

`Ctrl+C` dans le terminal 1 arrête tous les processus et libère toutes les ressources.

Organisation du code (`src/tp_1_Anton_Dion_Arnaud_Wissoqc/`) :

| Fichier | Rôle |
|---|---|
| `starter_serveur.py` | watch-dog : crée les ressources, lance le serveur principal et le relance s'il tombe |
| `serveur_principal.py` | serveur principal **d** (dispatcher) |
| `serveur_secondaire.py` | serveur secondaire **w** (worker) |
| `segment.py` | segment mémoire partagé entre d et w |
| `outils.py` | protocole texte des sockets (lignes bornées) |
| `systeme.py` | création de processus (`fork`), attente bornée d'un fils, suppression de fichier |
| `client.py` | client |
| `config.py` | paramètres (ports, délais, limites) |
| `test_projet.py` | test fonctionnel |

---

## 2. Architecture

```
   watch-dog  (crée : segment mémoire partagé, dossier privé des tubes)
       |  fork + waitpid
       v
   serveur principal d  <--TCP 2222--  client
       |  fork + waitpid      |
       |   dwtube1 (d -> w)   |
       |   wdtube1 (w -> d)   |
       v                      |
   serveur secondaire w  <--TCP 2223---+
            segment mémoire partagé entre d et w
```

Chaque élément est un processus lourd : le watch-dog, d, w et chaque client. Il n'y a aucun thread, conformément au §6 du sujet.

---

## 3. Segment mémoire partagé (partie 1)

### 1.a – Quel module utiliser ?

Nous utilisons **`sysv_ipc`** (SHM System V), l'un des deux modules étudiés en cours, à la place de `multiprocessing.shared_memory` (« SHM à la Python »). Il est maintenu (version 1.2.0) et permet en une ligne :

- de **fixer les droits** du segment (`mode=0o600`, propriétaire seul) ;
- une **création exclusive avec une clé aléatoire** (`key=None`, `IPC_CREX`). Avec une clé fixe (notre première version utilisait 4321), un autre utilisateur pourrait créer le segment avant nous et nous faire travailler dans un segment qu'il contrôle.

Contenu du segment (taille fixe, 50 octets) :

| Champ | Taille | Écrit par | Contenu |
|---|---|---|---|
| request | 16 | d | type de requête (`requetetype1` / `requetetype2`) |
| client | 16 | d | adresse IP du client |
| status | 16 | w | `debuttraitement`, `fintraitement` ou `errtraitement` |
| port | 2 | w | port du serveur secondaire |

Le contenu du segment n'est **jamais considéré comme fiable** : chaque lecture vérifie le type de requête et la valeur du statut.

Un piège rencontré en testant : `sysv_ipc` remplit un nouveau segment d'**espaces**, pas de zéros. Notre premier compteur valait 2 314 885 530 818 453 537 (0x2020202020202021). Le segment est donc créé avec `init_character=b"\0"`.

---

## 4. Paire de tubes nommés et synchronisation (partie 2)

### Principe : un jeton qui fait l'aller-retour

La paire **unique** `dwtube1` (d → w) / `wdtube1` (w → d) est créée dans un dossier privé (droits `0700`, nom aléatoire) avec les droits `0600`.

L'exclusion mutuelle repose sur une règle simple : **seul le processus qui a reçu le dernier message du ping-pong peut utiliser le segment.**

- d n'écrit dans le segment qu'**avant** d'envoyer `ping`, et ne le lit qu'**après** avoir reçu `pong` ;
- w ne le lit et n'y écrit qu'entre un `ping` reçu et le `pong` qu'il renvoie.

Les messages alternant strictement, d et w ne peuvent jamais utiliser le segment en même temps. Aucun sémaphore n'est nécessaire.

Déroulement d'une requête (deux allers-retours) :

| Étape | d | Tube | w |
|---|---|---|---|
| 1 | écrit la requête et l'adresse du client dans le segment | `ping` → | attend |
| 2 | attend | ← `pong` | lit la requête, écrit `debuttraitement` et son port |
| 3 | lit le port et le transmet au client | `ping` → | attend le client |
| 4 | attend (refuse les autres clients) | ← `pong` | sert le client, écrit `fintraitement` ou `errtraitement` |

### Pourquoi l'exemple Moodle ne fonctionne pas, et solutions

Les causes classiques de blocage d'un échange par tubes nommés sont les suivantes. Elles sont à confronter à l'exemple Moodle.

1. **`open()` sur un tube nommé est bloquant** tant que l'autre extrémité n'est pas ouverte. Si les deux processus ouvrent leurs tubes dans un ordre croisé (chacun ouvre d'abord le tube que l'autre ouvre en second), chacun attend l'autre : c'est un **interblocage**.
2. **Écritures bufférisées** : avec `open(..., "w")`, le texte reste dans le tampon Python tant qu'il n'y a pas de `flush()`. Le lecteur attend indéfiniment.
3. **Lecture par ligne** : `readline()` attend un `\n`. Un message sans fin de ligne bloque le lecteur.
4. Les deux processus peuvent aussi se mettre à **lire en même temps**, chacun attendant un message de l'autre.

Solutions possibles : même ordre d'ouverture dans les deux processus, ouverture en lecture avec `O_NONBLOCK`, `flush()` après chaque écriture, messages terminés par `\n`, alternance stricte des messages.

**Notre solution** : d ouvre les **quatre extrémités avant le `fork()`**, chaque lecture d'abord en `O_NONBLOCK`, ce qui rend l'ouverture en écriture immédiate. Aucun `open()` ne bloque. Les écritures passent par `os.write`, sans tampon, et chaque processus ferme ensuite les extrémités qui ne le concernent pas.

### 2.a – Arrêter proprement les serveurs après N échanges

Le paramètre `MAX_REQUESTS` (dans `config.py`) arrête le service après N requêtes traitées ; 0 signifie sans limite. d envoie alors `stop` sur `dwtube1`. w termine sa boucle, ferme ses ressources et se termine avec le code 0. d récupère w par `waitpid()`, supprime les tubes et se termine avec le code 0. Le watch-dog voit ce code 0, ne relance pas d et supprime le segment.

### 2.b – Qui doit s'arrêter en premier ? Qu'est-ce qu'un zombie ?

Un **zombie** est un processus terminé dont le père n'a pas encore récupéré le code de retour (`wait`/`waitpid`). Il ne s'exécute plus, mais son entrée reste dans la table des processus (état `Z` dans `ps`). En grand nombre, les zombies épuisent la table des processus.

**Le fils doit s'arrêter en premier**, donc w avant d, puis d avant le watch-dog. Le père, encore vivant, récupère alors son code de retour par `waitpid()`. Si le père s'arrêtait d'abord, le fils deviendrait orphelin, rattaché à `init`/`systemd`, et continuerait à tourner hors de tout contrôle.

Dans le projet :
- d récupère w avec `waitpid()` dans sa boucle et à l'arrêt ;
- le watch-dog récupère d de la même façon ;
- le test lance des clients simultanés avec `fork()` et les récupère tous.

Aucun zombie n'a été observé pendant les tests.

---

## 5. Processus de surveillance (partie 3)

### Fonctionnement

Nous avons choisi une surveillance **simple**, qui vérifie qu'un serveur n'est **pas tombé**. Elle s'appuie sur `waitpid()`, la façon dont le système informe un père de la fin de son fils :

- le **watch-dog** attend d avec `waitpid()`. Si d se termine sur une erreur (code différent de 0), il est relancé après 2 s avec un nouveau segment, au plus 3 fois ;
- **d** vérifie w avec `waitpid(..., WNOHANG)` toutes les 0,5 s. Si w est tombé, d le récupère, recrée les tubes et le relance après 2 s, au plus 3 fois. Un client en attente reçoit alors `errtraitement`.

### 3.a – Lancer d depuis le watch-dog, puis w depuis d ?

Oui, c'est notre choix. Seul le **père** d'un processus peut récupérer son code de retour avec `waitpid()`. Le watch-dog ne peut donc ni récupérer ni relancer w, qui n'est pas son fils. **Le redémarrage de w est délégué à d**. La perte de réactivité est faible : d vérifie l'état de w toutes les 0,5 s.

Le risque est que, si d tombe, w ne soit plus surveillé. Mais quand d s'arrête, ses extrémités de tubes se ferment. L'ancien w lit alors une fin de fichier sur `dwtube1` et s'arrête de lui-même, puis le nouveau d lance un nouveau w. Le secondaire est donc bien relancé après un redémarrage du principal (testé).

### 3.b – Utiliser les tubes d ↔ w pour le watch-dog ?

Non, c'est une mauvaise idée :

- **Un tube n'a pas d'adressage.** Si le watch-dog écrit aussi dans `wdtube1`, d peut prendre son message pour un `pong` et croire qu'il possède le segment alors que w l'utilise encore. L'alternance stricte est rompue, donc l'exclusion mutuelle aussi : **corruption du segment**.
- Plusieurs lecteurs sur un même tube se « volent » les messages.
- **Un w figé ne lit plus `dwtube1`** : l'ordre de redémarrage ne sera jamais lu. Un processus figé ne peut pas se relancer lui-même.
- Le tube se remplit et le watch-dog se bloque en écriture (**interblocage**).
- Tout processus ayant accès au tube pourrait injecter de faux ordres.

### 3.c – Utiliser des signaux ?

Les signaux ne sont pas adaptés à la communication :

- ils sont **asynchrones** et interrompent n'importe quel appel système ;
- le gestionnaire ne peut faire que des opérations très limitées (fonctions *async-signal-safe*) ;
- ils **ne transportent pas de données** ;
- les signaux standards **ne sont pas mis en file** : deux `SIGUSR1` rapprochés peuvent n'en faire qu'un ;
- un PID peut être **réutilisé**, si bien qu'on risque de signaler un autre processus ;
- tout processus du même utilisateur peut en envoyer, ce qui permet d'usurper un serveur.

**Oui, le père est informé de l'arrêt d'un fils** par le signal `SIGCHLD`, ignoré par défaut. Il récupère ensuite le code de retour avec `waitpid()`. C'est ce `waitpid()` que nous utilisons, sans gérer de signal : **le projet n'utilise aucun signal.**

### Troisième idée : files de messages (MSQ)

Une file de messages entre le watch-dog et les serveurs permettrait de détecter un serveur **figé** : le watch-dog envoie régulièrement un `ping` adressé (grâce au type du message) et attend une réponse. Nous ne l'avons **pas retenue** pour garder une surveillance simple : notre watch-dog détecte un serveur **tombé**, pas un serveur figé. C'est la principale piste d'amélioration.

---

## 6. Client et protocole (parties 4 et 5)

Toutes les communications sur les sockets sont des lignes de texte UTF-8 terminées par `\n`, d'au plus 256 octets, sans caractère de contrôle.

| Étape du sujet | Réalisation |
|---|---|
| 1. Requête du client à d | le client se connecte en TCP au port **2222** et envoie `requetetype1` ou `requetetype2` |
| 2. Délégation de d à w | d écrit la requête et l'adresse du client dans le segment, puis envoie `ping` |
| 3. Acceptation par w | w écrit `debuttraitement` et son port (2223) dans le segment, puis envoie `pong` |
| 4. Transmission au client | d répond `port 2223` au client et coupe la connexion |
| 5. Données du client à w | le client se connecte au port 2223 et envoie ses données |
| 6. Résultat | w vérifie que l'adresse du client est celle annoncée par d, simule le traitement, répond `resultat ...` |
| 7. Fin de traitement | w écrit `fintraitement` (ou `errtraitement`) dans le segment, puis envoie `pong` ; w est de nouveau disponible |

Réponses possibles de d : `port 2223`, `occupe`, `refus` (type de requête inconnu) ou `errtraitement`.

**Refuser un deuxième client (4.2)** : w est unique. Tant qu'une requête est en cours, d répond `occupe` à tout nouveau client et ferme la connexion.

**Course corrigée pendant les tests** : un client qui renvoie une requête dès réception de son résultat recevait parfois `occupe`. d traitait la nouvelle connexion avant le `pong` de fin de traitement arrivé dans la même itération de sa boucle. d lit désormais les tubes avant d'accepter les clients. Un client peut tout de même recevoir `occupe` s'il arrive pendant un traitement : il doit alors réessayer.

Le client ne fait pas confiance à la réponse de d. Il vérifie que le port est un nombre compris entre 2223 et 65535, et se reconnecte toujours à l'adresse configurée, jamais à une adresse fournie par le serveur.

---

## 7. Contraintes de sécurité du sujet (§6)

| Contrainte | Réalisation |
|---|---|
| Codes de retour et exceptions traités partout | chaque appel système est dans un `try` ciblé ; chaque processus se termine avec un code (0 ou 1) |
| Ressource demandée = ressource vérifiée | création exclusive (`IPC_CREX`) du segment ; `mkfifo` et `mkdtemp` lèvent une exception en cas d'échec |
| Droits limités au strict nécessaire | `umask 077` ; segment et tubes en `0600` ; dossier des tubes en `0700` ; refus de s'exécuter en root |
| Libération dans l'ordre inverse de l'acquisition, même sur exception | blocs `finally` : w, puis les tubes, la socket et le contenu du segment (dans d) ; puis le segment et le dossier (dans le watch-dog) |
| Arrêt ordonné, pas de zombie | w s'arrête avant d, d avant le watch-dog ; chaque père fait `waitpid()` |
| Paramètres contrôlés | messages bornés (`readline` limité), caractères de contrôle refusés, types de requête dans une liste fermée, port reçu vérifié, contenu du segment validé à la lecture |
| Accès inutiles supprimés après `fork()` | w ferme la socket d'écoute de d et les extrémités de tubes de d ; d ferme celles de w |
| Pas d'exécution multiple non souhaitée | tous les `fork()` passent par `fork_process()` (`systeme.py`) : le fils termine par `os._exit()` et ne revient jamais dans le code du père |
| Ressources persistantes supprimées ou effacées | segment rempli de zéros puis supprimé ; tubes et dossier supprimés à l'arrêt |
| Pas de requêtes asynchrones ni de threads | sockets bloquantes avec délais d'attente et `select()` ; aucun thread, y compris dans le test |

Autres mesures :
- écoute sur `127.0.0.1` uniquement ;
- délais d'attente sur toutes les sockets : un client muet ne bloque pas les serveurs ;
- w ne sert que le client dont d lui a transmis l'adresse ;
- les journaux ne contiennent jamais le contenu des messages, seulement leur taille ou leur type.

---

## 8. Tests réalisés

Tous les tests ont été faits avec un compte non privilégié, via `uv run`.

| Test | Résultat |
|---|---|
| `uv run tp-1-client requetetype2 "carte 7"` | `resultat requetetype2 : 7 caractères traités` |
| `uv run tp-1-test`, lancé 10 fois de suite | 10/10 OK : 3 requêtes successives servies ; 3 clients simultanés (`fork`) = 1 servi, 2 `occupe` |
| Type de requête inconnu | `refus` |
| Ligne de 10 Ko, séquence d'échappement ANSI | connexion fermée, cause journalisée |
| `kill -9` de w | `secondary server ... down`, relancé par d après 2 s ; test OK |
| `kill -9` de d | `principal server down`, relancé par le watch-dog ; l'ancien w s'arrête seul ; nouveau w lancé ; test OK |
| `MAX_REQUESTS = 2` | arrêt normal après 2 requêtes |
| `Ctrl+C` | aucun processus, segment (`ipcs -m`) ni tube restant ; aucun zombie |

Les commandes `kill` ne servent qu'à **simuler** des pannes pendant les tests ; le projet lui-même n'envoie aucun signal.

---

## 9. Limites connues

- Un serveur **figé** (bloqué mais toujours vivant) n'est pas détecté : la surveillance vérifie seulement qu'un serveur n'est pas tombé. Si w est figé pendant un traitement, d répond `occupe` à tous les clients. Les files de messages (§5) seraient la suite logique.
- L'arrêt se fait par `Ctrl+C`, ce qui suppose un lancement au premier plan dans un terminal.
- Les échanges ne sont pas chiffrés : l'écoute est limitée à la machine locale (`127.0.0.1`).
- Le protocole complet sera détaillé plus tard dans le projet ; le traitement est pour l'instant une simple temporisation.
