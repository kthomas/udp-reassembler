package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func emit() []string {
	var messageIds [10]string
	cmd := exec.Command("./udp_emitter.sh", os.Getenv("PATH"))
	stdout, _ := cmd.CombinedOutput()
	lines := strings.Split(string(stdout), "\n")
	for i := range lines {
		line := lines[i]
		lineParts := strings.Split(line, ":")
		sha256 := lineParts[len(lineParts) - 1]
		copy(messageIds[i:], []string { sha256 })
	}
	return messageIds[:]
}

func TestReassembleMessages(t *testing.T) {
	go ReassembleMessages()
	messageSHAs := emit()

	idx := 1
	for k, _ := range messages {
		sha256 := getMessageSHA(k)
		buf := getMessage(k)
		matched := false
		for i := range messageSHAs {
			if strings.Compare(messageSHAs[i], sha256) == 0 {
				matched = true
			}
			if matched {
				continue
			}
		}
		if !matched {
			t.Logf("Failed to match %s", sha256)
			holes := findHoles([]byte(buf))
			for hole := range holes {
				t.Logf("Message #%v hole at: %v", idx, holes[hole])
			}

			t.Fail()
		} else {
			t.Logf("Message #%v length: %v sha256:%s", idx, len(buf), sha256)
		}
		idx++
	}
}
