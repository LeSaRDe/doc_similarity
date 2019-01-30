
JFLAGS = -g -d bin -sourcepath src
JC = javac
RM = rm

SRCPATH = src/usersimproj

.SUFFIXES: .java .class

.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
		$(SRCPATH)/CoreNLPWrap.java \
		$(SRCPATH)/DeSentence.java \
		$(SRCPATH)/DeToken.java \
		$(SRCPATH)/UserTextIn.java \
		$(SRCPATH)/UserTextInFile.java \
		$(SRCPATH)/UserTextRec.java \
		$(SRCPATH)/UserTextTask.java \
		$(SRCPATH)/UserSimConstants.java \
		$(SRCPATH)/AnnotateUserText.java \
		$(SRCPATH)/TestClient.java \
		$(SRCPATH)/UserSimTest.java

#$(SRCPATH)/StopwordAnnotator.java \
#$(SRCPATH)/WordSimTask.java \
#$(SRCPATH)/WordSimServer.java \
#$(SRCPATH)/BabelWrap.java \
#$(SRCPATH)/ADWWrap.java \

default: classes

classes: $(CLASSES:.java=.class)

env:
	export CLASSPATH=./bin:${CLASSPATH}

clean:
	$(RM) -rf bin/*
