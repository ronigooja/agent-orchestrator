package chat_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	"time"
)

type independentRetryProofConversation struct {
	*fakeConversation
	attempts int
}

func (c *independentRetryProofConversation) CanTerminateWithProof() bool { return true }
func (c *independentRetryProofConversation) Terminate() error {
	c.attempts++
	if c.attempts == 1 {
		return errors.New("synthetic container stop unverified")
	}
	return c.fakeConversation.Close()
}
func (c *independentRetryProofConversation) TerminateWithProof() (ports.ChatTerminationProof, error) {
	if err := c.Terminate(); err != nil {
		return ports.ChatTerminationProof{}, err
	}
	return ports.ChatTerminationProof{SessionID: string(testSession), Method: ports.ChatTerminationNativeProcessExitWaited, CleanupComplete: true, VerifiedAt: time.Now().UTC()}, nil
}
func TestStopChatWithProofRetriesSameControllerAfterStopFailure(t *testing.T) {
	conversation := &independentRetryProofConversation{fakeConversation: newFakeConversation()}
	harness := newHarnessWithConversation(t, conversation)
	t.Cleanup(func() { _ = conversation.fakeConversation.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := harness.svc.StopChatWithProof(ctx, testSession); err == nil {
		t.Fatal("first unverified stop returned proof")
	}
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer retryCancel()
	proof, err := harness.svc.StopChatWithProof(retryCtx, testSession)
	if err != nil || !proof.CleanupComplete {
		t.Fatalf("same exact controller retry failed: attempts=%d proof=%+v err=%v", conversation.attempts, proof, err)
	}
}

func TestStopChatWithProofRetriesAfterOrdinaryStopFailure(t *testing.T) {
	conversation := &independentRetryProofConversation{fakeConversation: newFakeConversation()}
	harness := newHarnessWithConversation(t, conversation)
	t.Cleanup(func() { _ = conversation.fakeConversation.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := harness.svc.Stop(ctx, testSession); err == nil {
		t.Fatal("first ordinary stop unexpectedly succeeded")
	}
	retryCtx, retryCancel := context.WithTimeout(context.Background(), time.Second)
	defer retryCancel()
	proof, err := harness.svc.StopChatWithProof(retryCtx, testSession)
	if err != nil || !proof.CleanupComplete || conversation.attempts < 2 {
		t.Fatalf("required retry after ordinary failure: attempts=%d cleanup=%t err=%v", conversation.attempts, proof.CleanupComplete, err)
	}
}

type gatedProofConversation struct {
	*independentRetryProofConversation
	entered chan struct{}
	release chan struct{}
}

func (c *gatedProofConversation) TerminateWithProof() (ports.ChatTerminationProof, error) {
	close(c.entered)
	<-c.release
	return c.independentRetryProofConversation.TerminateWithProof()
}

func TestStopChatWithProofHoldsControllerGateThroughProofAndCleanup(t *testing.T) {
	conversation := &gatedProofConversation{
		independentRetryProofConversation: &independentRetryProofConversation{fakeConversation: newFakeConversation()},
		entered:                           make(chan struct{}), release: make(chan struct{}),
	}
	harness := newHarnessWithConversation(t, conversation)
	conversation.attempts = 1 // model a prior ordinary stop that failed
	t.Cleanup(func() { _ = conversation.fakeConversation.Close() })
	proofDone := make(chan error, 1)
	go func() {
		_, err := harness.svc.StopChatWithProof(context.Background(), testSession)
		proofDone <- err
	}()
	select {
	case <-conversation.entered:
	case <-time.After(time.Second):
		t.Fatal("proof call did not reach provider")
	}
	startDone := make(chan error, 1)
	go func() {
		_, err := harness.svc.Start(context.Background(), chatsvc.StartConfig{
			SessionID: testSession, ProjectID: testProject, Harness: domain.HarnessCodex, WorkspacePath: t.TempDir(),
		})
		startDone <- err
	}()
	select {
	case err := <-startDone:
		t.Fatalf("same-owner start crossed in-flight proof gate: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(conversation.release)
	select {
	case err := <-proofDone:
		if err != nil {
			t.Fatalf("required proof stop failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("proof stop did not finish after provider release")
	}
	select {
	case <-startDone:
	case <-time.After(time.Second):
		t.Fatal("same-owner start remained blocked after proof cleanup")
	}
}
