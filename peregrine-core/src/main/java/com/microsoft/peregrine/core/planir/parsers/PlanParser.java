package com.microsoft.peregrine.core.planir.parsers;

import com.microsoft.peregrine.core.planir.parsers.entities.Operator;
import com.microsoft.peregrine.core.planir.parsers.entities.Plan;

public abstract class PlanParser<Input> implements IParser<Input, Plan> {
  protected void getOpTree(String[] lines, int idx, int indent, Operator root) {
    if (idx >= lines.length)
      return; 
    if (lines[idx].isEmpty())
      getOpTree(lines, idx + 1, indent, root); 
    int newIndent = getIndentation(lines[idx]);
    Operator op = getOp(lines[idx].substring(newIndent));
    op.indent = newIndent;
    if (op.name.isEmpty() || op.name.equals("where") || op.name.equals("and")) {
      root.addAdditionalPredicates(op.name + " " + op.opInfo);
      newIndent = indent;
      op = root;
    } else if (newIndent > indent) {
      if (root != null)
        root.addChild(op); 
    } else if (newIndent == indent) {
      root.parent.addChild(op);
    } else {
      for (Operator sibling = root; sibling != null; sibling = sibling.parent) {
        if (newIndent == sibling.indent || sibling.parent instanceof Operator.RootOp) {
          sibling.parent.addChild(op);
          break;
        } 
      } 
    } 
    getOpTree(lines, idx + 1, newIndent, op);
  }
  
  private int getIndentation(String line) {
    for (int i = 0; i < line.length(); i++) {
      if (Character.isLetter(line.charAt(i)) || 
        Character.isDigit(line.charAt(i)) || line
        .charAt(i) == '*')
        return i; 
    } 
    return line.length();
  }
  
  public abstract Plan parse(Input paramInput);
  
  protected abstract Operator getOp(String paramString);
}
