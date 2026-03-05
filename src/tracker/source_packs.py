from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RssPack:
    id: str
    label: str
    urls: list[str]


_HN_POPULARITY_KARPATHY_URLS: list[str] = [
    "https://simonwillison.net/atom/everything/",
    "https://www.jeffgeerling.com/blog.xml",
    "https://www.seangoedecke.com/rss.xml",
    "https://krebsonsecurity.com/feed/",
    "https://daringfireball.net/feeds/main",
    "https://ericmigi.com/rss.xml",
    "http://antirez.com/rss",
    "https://idiallo.com/feed.rss",
    "https://maurycyz.com/index.xml",
    "https://pluralistic.net/feed/",
    "https://shkspr.mobi/blog/feed/",
    "https://lcamtuf.substack.com/feed",
    "https://mitchellh.com/feed.xml",
    "https://dynomight.net/feed.xml",
    "https://utcc.utoronto.ca/~cks/space/blog/?atom",
    "https://xeiaso.net/blog.rss",
    "https://devblogs.microsoft.com/oldnewthing/feed",
    "https://www.righto.com/feeds/posts/default",
    "https://lucumr.pocoo.org/feed.atom",
    "https://skyfall.dev/rss.xml",
    "https://garymarcus.substack.com/feed",
    "https://rachelbythebay.com/w/atom.xml",
    "https://overreacted.io/rss.xml",
    "https://timsh.org/rss/",
    "https://www.johndcook.com/blog/feed/",
    "https://gilesthomas.com/feed/rss.xml",
    "https://matklad.github.io/feed.xml",
    "https://www.theatlantic.com/feed/author/derek-thompson/",
    "https://evanhahn.com/feed.xml",
    "https://terriblesoftware.org/feed/",
    "https://rakhim.exotext.com/rss.xml",
    "https://joanwestenberg.com/rss",
    "https://xania.org/feed",
    "https://micahflee.com/feed/",
    "https://nesbitt.io/feed.xml",
    "https://www.construction-physics.com/feed",
    "https://feed.tedium.co/",
    "https://susam.net/feed.xml",
    "https://entropicthoughts.com/feed.xml",
    "https://buttondown.com/hillelwayne/rss",
    "https://www.dwarkeshpatel.com/feed",
    "https://borretti.me/feed.xml",
    "https://www.wheresyoured.at/rss/",
    "https://jayd.ml/feed.xml",
    "https://minimaxir.com/index.xml",
    "https://geohot.github.io/blog/feed.xml",
    "http://www.aaronsw.com/2002/feeds/pgessays.rss",
    "https://www.filfre.net/feed/",
    "https://blog.jim-nielsen.com/feed.xml",
    "https://dfarq.homeip.net/feed/",
    "https://jyn.dev/atom.xml",
    "https://www.geoffreylitt.com/feed.xml",
    "https://www.downtowndougbrown.com/feed/",
    "https://brutecat.com/rss.xml",
    "https://eli.thegreenplace.net/feeds/all.atom.xml",
    "https://www.abortretry.fail/feed",
    "https://fabiensanglard.net/rss.xml",
    "https://oldvcr.blogspot.com/feeds/posts/default",
    "https://bogdanthegeek.github.io/blog/index.xml",
    "https://hugotunius.se/feed.xml",
    "https://gwern.substack.com/feed",
    "https://berthub.eu/articles/index.xml",
    "https://chadnauseam.com/rss.xml",
    "https://simone.org/feed/",
    "https://it-notes.dragas.net/feed/",
    "https://beej.us/blog/rss.xml",
    "https://hey.paris/index.xml",
    "https://danielwirtz.com/rss.xml",
    "https://matduggan.com/rss/",
    "https://refactoringenglish.com/index.xml",
    "https://worksonmymachine.substack.com/feed",
    "https://philiplaine.com/index.xml",
    "https://steveblank.com/feed/",
    "https://bernsteinbear.com/feed.xml",
    "https://danieldelaney.net/feed",
    "https://www.troyhunt.com/rss/",
    "https://herman.bearblog.dev/feed/",
    "https://tomrenner.com/index.xml",
    "https://blog.pixelmelt.dev/rss/",
    "https://martinalderson.com/feed.xml",
    "https://danielchasehooper.com/feed.xml",
    "https://www.chiark.greenend.org.uk/~sgtatham/quasiblog/feed.xml",
    "https://grantslatton.com/rss.xml",
    "https://www.experimental-history.com/feed",
    "https://anildash.com/feed.xml",
    "https://aresluna.org/main.rss",
    "https://michael.stapelberg.ch/feed.xml",
    "https://blog.miguelgrinberg.com/feed",
    "https://keygen.sh/blog/feed.xml",
    "https://mjg59.dreamwidth.org/data/rss",
    "https://computer.rip/rss.xml",
    "https://www.tedunangst.com/flak/rss",
]


def list_rss_packs() -> list[RssPack]:
    return [
        RssPack(
            id="hn_popularity_karpathy",
            label="HN Popularity (Karpathy 90+ blogs)",
            urls=list(_HN_POPULARITY_KARPATHY_URLS),
        )
    ]


def get_rss_pack(pack_id: str) -> RssPack:
    pid = (pack_id or "").strip().lower()
    aliases = {
        "hn-popularity": "hn_popularity_karpathy",
        "hn_popularity": "hn_popularity_karpathy",
        "karpathy": "hn_popularity_karpathy",
        "karpathy90": "hn_popularity_karpathy",
        "karpathy_90": "hn_popularity_karpathy",
        "karpathy92": "hn_popularity_karpathy",
        "karpathy_92": "hn_popularity_karpathy",
    }
    pid = aliases.get(pid, pid)

    for p in list_rss_packs():
        if p.id == pid:
            return p
    raise ValueError(f"unknown rss pack: {pack_id}")

