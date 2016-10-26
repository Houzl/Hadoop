# PageRank Iterator.
The input and output format should be like following (Split by single space) :

PageA PR(PageA) OutLink1 OutLink2 OutLink3 ... OutLinkN

Output for Mapper (key, value):
PageA, "##OL##" + OutLink1 OutLink2 OutLink3 ... OutLinkN
OutLink1, "##PR##" + PR(PageA)/N
OutLink2, "##PR##" + PR(PageA)/N
OutLink3, "##PR##" + PR(PageA)/N
...
OutLinkN, "##PR##" + PR(PageA)/N

Output for Reducer:
PageA newPR(PageA) OutLink1 OutLink2 OutLink3 ... OutLinkN

newPR(PageA) used following formula:
![PR(A) = (1-d)/N + d(PR(B)/l(B) + PR(B)/l(B) + PR(B)/l(B) + ...)](https://wikimedia.org/api/rest_v1/media/math/render/svg/7c3da6d608ba21cac0bbfc96e59615ffe8f33360 "PageRank scores formula from wikipedia")

PageRankTestData, Comes from https://en.wikipedia.org/wiki/PageRank: should be:
A 0.1 
B 0.1 C
C 0.1 B
D 0.1 A B
E 0.1 B D F
F 0.1 B E
G 0.1 B E
H 0.1 B E
I 0.1 B E
J 0.1 E
K 0.1 E

The output after 5 iterations should be (N = 11):
-0.3910171254997895	B
-0.2531458729482323	C
-0.0723527886679293	E
-0.033094285222011785	D
-0.033094285222011785	F
-0.03178460684974748	A
-0.01363636363636364	G
-0.01363636363636364	H
-0.01363636363636364	I
-0.01363636363636364	J
-0.01363636363636364	K


