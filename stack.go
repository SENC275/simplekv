package simplekv

type Stack struct {
	stack []*treeNode
	base  int
	top   int
}

func InitStack(n int) Stack {
	stack := Stack{
		stack: make([]*treeNode, n),
	}
	return stack
}

func (stack *Stack) Push(value *treeNode) {
	if stack.top == len(stack.stack) {
		stack.stack = append(stack.stack, value)
	} else {
		stack.stack[stack.top] = value
	}
	stack.top++
}

func (stack *Stack) Pop() (*treeNode, bool) {
	if stack.top == stack.base {
		return nil, false
	}
	stack.top--
	return stack.stack[stack.top], true
}
