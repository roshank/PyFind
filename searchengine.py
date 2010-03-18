import urllib2
import httplib
import socket
from BeautifulSoup import *
from urlparse import urljoin
from sqlite3 import dbapi2 as sqlite

ignorewords=set(['the','of','to','and','a','in','is','it'])
max_crawl = 100000

TIMEOUT = 5  # socket timeout

BROWSERS = (
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.6) Gecko/2009011913 Firefox/3.0.6',
    'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.0.6) Gecko/2009011912 Firefox/3.0.6',
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.6) Gecko/2009011913 Firefox/3.0.6 (.NET CLR 3.5.30729)',
    'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.6) Gecko/2009020911 Ubuntu/8.10 (intrepid) Firefox/3.0.6',
    'Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US; rv:1.9.0.6) Gecko/2009011913 Firefox/3.0.6',
    'Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US; rv:1.9.0.6) Gecko/2009011913 Firefox/3.0.6 (.NET CLR 3.5.30729)',
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/525.19 (KHTML, like Gecko) Chrome/1.0.154.48 Safari/525.19',
    'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648)',
    'Mozilla/5.0 (X11; U; Linux x86_64; en-US; rv:1.9.0.6) Gecko/2009020911 Ubuntu/8.10 (intrepid) Firefox/3.0.6',
    'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.5) Gecko/2008121621 Ubuntu/8.04 (hardy) Firefox/3.0.5',
    'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_6; en-us) AppleWebKit/525.27.1 (KHTML, like Gecko) Version/3.2.1 Safari/525.27.1',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727)',
    'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
)

DOMAIN_RULES = dict({'.':'twitter.com','/status/':'@@@@'})

class PoolHTTPConnection(httplib.HTTPConnection):
    def connect(self):
        """Connect to the host and port specified in __init__."""
        msg = "getaddrinfo returns an empty list"
        for res in socket.getaddrinfo(self.host, self.port, 0,
                                      socket.SOCK_STREAM):
            af, socktype, proto, canonname, sa = res
            try:
                self.sock = socket.socket(af, socktype, proto)
                if self.debuglevel > 0:
                    print "connect: (%s, %s)" % (self.host, self.port)
                self.sock.settimeout(TIMEOUT)
                self.sock.connect(sa)
            except socket.error, msg:
                if self.debuglevel > 0:
                    print 'connect fail:', (self.host, self.port)
                if self.sock:
                    self.sock.close()
                self.sock = None
                continue
            break
        if not self.sock:
            raise socket.error, msg
        
class PoolHTTPHandler(urllib2.HTTPHandler):
    def http_open(self, req):
        return self.do_open(PoolHTTPConnection, req)

    

class crawler:
    def __init__(self, dbname, user_agent=BROWSERS[7]):
        self.headers = {
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-us,en;q=0.5'
        }
        self.con=sqlite.connect(dbname)

    def __del__(self):
        self.con.close()
        
    def dbcommit(self):
        self.con.commit()

    def calculatepagerank(self, iterations=20):
        # clear current pagerank tables
        self.con.execute('drop table if exists pagerank')
        self.con.execute('create table pagerank(urlid primary key, score)')

        self.con.execute('insert into pagerank select rowid, 1.0 from urllist')
        self.dbcommit()

        for i in range(iterations):
            print "iteraration %d" % (i)
            for (urlid,) in self.con.execute('select rowid from urllist'):
                pr=0.15

                for (linker,) in self.con.execute('select distinct fromid from link where toid=%d' % urlid):
                    linkingpr = self.con.execute('select score from pagerank where urlid=%d' %linker).fetchone()[0]

                    linkingcount = self.con.execute('select count(*) from link where fromid=%d' %linker).fetchone()[0]
                    pr+=0.85*(linkingpr/linkingcount)
                self.con.execute('update pagerank set score=%f where urlid=%d' % (pr, urlid))
            self.dbcommit()        

    def getentryid(self, table, field, value, createnew=True):
        cur = self.con.execute("select rowid from %s where %s='%s'" % (table, field, value))
        res = cur.fetchone()
        if res==None:
            cur = self.con.execute("insert into %s (%s) values ('%s')" % (table, field, value))
            return cur.lastrowid
        else:
            return res[0]

    def addtoindex(self, url, soup):
        if self.isindexed(url): return
        print 'Indexing ' +url

        # get words
        text = self.gettextonly(soup)
        words = self.seperatewords(text)

        # get url id
        urlid = self.getentryid('urllist', 'url', url)

        # link words to url
        for i in range(len(words)):
            word = words[i]
            if word in ignorewords : continue
            wordid = self.getentryid('wordlist', 'word', word)
            self.con.execute("insert into wordlocation(urlid, wordid, location) \
                values (%d, %d, %d)" % (urlid, wordid, i))
        
        
    def gettextonly(self, soup):
        v = soup.string
        if v == None:
            c = soup.contents
            resulttext=''
            for t in c:
                subtext = self.gettextonly(t)
                resulttext+=subtext+'\n'
            return resulttext
        else:
            return v.strip()
        
    def seperatewords (self, text):
        splitter = re.compile('\\W*')
        return [s.lower() for s in splitter.split(text) if s!='']
        
    def isindexed(self, url):
        u = self.con.execute("select rowid from urllist where url='%s'" %url).fetchone()
        if u!=None:
            # check crawl status
            v = self.con.execute('select * from wordlocation where urlid=%d' % u[0]).fetchone()
            if v!=None: return True
        return False
        
    def addlinkref(self, urlFrom, urlTo, linkText):
        pass

    def crawl(self, pages, depth=2, reindex=False):
        crawled = 0
        handlers = [PoolHTTPHandler]
        opener = urllib2.build_opener(*handlers)
        for i in range(depth):
            newpages = set()
            for page in pages:
                crawled+=1
                if (crawled % 100 == 0):
                    print "\n\n!!! Crawled %d Pages !!! \n\n" % crawled
                if crawled > max_crawl:
                    return
                if (not reindex and self.isindexed(page)):
                    continue
                
                request = urllib2.Request(page, None, self.headers)
                try:
                    response = opener.open(request)
                    soup = BeautifulSoup(response.read())
                    self.addtoindex(page, soup)
                except:
                    print "Could not open %s" %page
                    continue
 
                links=soup('a')
                for link in links:
                    if ('href' in dict(link.attrs)):
                        url=urljoin(page,link['href'])
                        if url.find("'")!=-1: continue
                        url=url.split('#')[0] # remove location portion
                        goodpage = True
                        if url[0:4]=='http':
                            for rule in DOMAIN_RULES:
                                if (url.rfind(rule) != -1):
                                    if (url.rfind(DOMAIN_RULES[rule]) == -1):
                                        goodpage=False
                                        break
                            if (goodpage):
                                newpages.add(url)
                        linkText = self.gettextonly(link)
                        self.addlinkref(page,url,linkText)

                self.dbcommit()
            pages = newpages

    def createindextables(self):
        self.con.execute('create table urllist(url)')
        self.con.execute('create table wordlist(word)')
        self.con.execute('create table wordlocation (urlid, wordid, location)')
        self.con.execute('create table link(fromid integer, toid integer)')
        self.con.execute('create table linkwords(wordid, linkid)')
        self.con.execute('create index wordidx on wordlist(word)')
        self.con.execute('create index urlidx on urllist(url)')
        self.con.execute('create index wordurlidx on wordlocation(wordid)')
        self.con.execute('create index urltoidx on link(toid)')
        self.con.execute('create index urlfromidx on link(fromid)')



class searcher:
    def __init__(self, dbname):
        self.con=sqlite.connect(dbname)
    def __del__(self):
        self.con.close()

    def getmatchrows(self, q):
        #strings to build the query
        fieldlist = 'w0.urlid'
        tablelist = ''
        clauselist = ''
        wordids = []

        #split the words
        words = q.split(' ')
        tablenumber = 0

        for word in words:
            # get the word id
            wordrow = self.con.execute("select rowid from wordlist where word='%s'" % word).fetchone()

            if wordrow != None:
                wordid = wordrow[0]
                wordids.append(wordid)
                if tablenumber>0:
                    tablelist += ','
                    clauselist += ' and '
                    clauselist += 'w%d.urlid=w%d.urlid and ' % (tablenumber-1, tablenumber)
                fieldlist += ',w%d.location' % tablenumber
                tablelist += 'wordlocation w%d' % tablenumber
                clauselist += 'w%d.wordid=%d' % (tablenumber, wordid)
                tablenumber += 1

        # create the query
        if (fieldlist == '' or tablelist == '' or clauselist == ''):
            return None, wordids
        fullquery = 'select %s from %s where %s' % (fieldlist, tablelist, clauselist)
        print fullquery
        cur = self.con.execute(fullquery)
        rows = [row for row in cur]

        return rows, wordids
        
    def getscoredlist(self, rows, wordids):
        totalscores = dict([(row[0],0) for row in rows])

        weights=[(0.5, self.frequencyscore(rows)),
                 (1.0, self.locationscore(rows)),
                 (1.5, self.distancescore(rows)),
#                 (2.0, self.pagerankscore(rows)),
                 (2.0, self.linktextscore(rows, wordids))]


        for (weight, scores) in weights:
            for url in totalscores:
                totalscores[url] += weight * scores[url]

        return totalscores

    def geturlname(self, id):
        return self.con.execute("select url from urllist where rowid=%d" % id).fetchone()[0]

    def query(self, q):
        rows, wordids = self.getmatchrows(q)
        if (rows == None):
            print 'No results found.'
            return
        scores = self.getscoredlist(rows, wordids)
        rankedscores = sorted([(score, url) for (url, score) in scores.items()], reverse=1)
        for (score, urlid) in rankedscores[0:10]:
            print '%f\t%s' % (score, self.geturlname(urlid))

    def normalizescores(self, scores, smallIsBetter=0):
        vsmall = 0.00001 # avoid division by 0
        if smallIsBetter:
            minscore = min(scores.values())
            return dict([(u, float(minscore)/max(vsmall, l)) for (u,l) in scores.items()])
        else:
            maxscore = max(scores.values())
            if maxscore==0: maxscore=vsmall
            return dict([(u, float(c)/maxscore) for (u,c) in scores.items()])

    def frequencyscore(self, rows):
        counts = dict([(row[0],0) for row in rows])
        for row in rows: counts[row[0]]+=1
        return self.normalizescores(counts)

    def locationscore(self, rows):
        locations = dict([(row[0], 1000000) for row in rows])
        for row in rows:
            loc = sum(row[1:])
            if loc < locations[row[0]]: locations[row[0]]=loc
        return self.normalizescores(locations, smallIsBetter=1)

    def distancescore(self, rows):
        if len(rows[0])<=2: return dict([(row[0],1.0) for row in rows])
        mindistance = dict([(row[0], 1000000) for row in rows])

        for row in rows:
            dist = sum([abs(row[i]-row[i-1]) for i in range(2, len(row))])
            if dist < mindistance[row[0]]: mindistance[row[0]]=dist
        return self.normalizescores(mindistance, smallIsBetter=1)

    def inboundlinkscore(self, rows):
        uniqueurls = set([row[0] for row in rows])
        inboundcount = dict ([(u, self.con.execute('select count(*) from link where toid=%d' %u).fetchone()[0])
                              for u in uniqueurls])
        return self.normalizescores(inboundcount)

    def pagerankscore(self, rows):
        pageranks=dict([(row[0], self.con.execute('select score from pagerank where urlid=%d' % row[0]).fetchone()[0]) for row in rows])
        maxrank = max(pageranks.values())
        normalizedscores=dict([(u, float(l)/maxrank) for (u,l) in pageranks.items()])
        return normalizedscores

    def linktextscore(self, rows, wordids):
        linkscores=dict([(row[0], 0) for row in rows])
        for wordid in wordids:
            cur = self.con.execute('select link.fromid,link.toid from linkwords,link where wordid=%d and linkwords.linkid=link.rowid' %  wordid)
            for (fromid, toid) in cur:
                if toid in linkscores:
                    pr = self.con.execute('select score from pagerank where urlid=%d' % fromid).fetchone()[0]
                    linkscores[toid]+=pr
            maxscore = max(linkscores.values())
            if maxscore != 0:
                normalizedscores = dict([(u,float(l)/maxscore) for (u,l) in linkscores.items()])
            else:
                normalizedscores = dict([(u, float(0)) for (u,l) in linkscores.items()])
            return normalizedscores  
            

#pagelist = ['http://en.wikipedia.org/wiki/Ruffed_lemur']
c = crawler('twitter.db')
#c.createindextables()
#c.crawl(pagelist)
e = searcher('twitter.db')

        


    
