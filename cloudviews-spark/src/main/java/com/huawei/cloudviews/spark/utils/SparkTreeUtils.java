package com.huawei.cloudviews.spark.utils;

import com.huawei.cloudviews.spark.planir.parsers.entities.SparkOpJson;
import java.util.List;
import java.util.Stack;

public class SparkTreeUtils {
  public static SparkOpJson getTreeFromPreorder(List<SparkOpJson> preorder) {
    if (preorder.isEmpty())
      return null; 
    Stack<SparkOpJson> availableParents = new Stack<>();
    SparkOpJson parent = null;
    for (int i = 0; i < preorder.size(); i++) {
      SparkOpJson node = preorder.get(i);
      node.setIdentifier(i + 1);
      if (parent == null && !availableParents.isEmpty())
        parent = availableParents.pop(); 
      if (parent != null)
        parent.addChild(node); 
      if (node.numChildren == 0)
        parent = null; 
      if (node.numChildren == 1)
        parent = node; 
      if (node.numChildren == 2) {
        parent = node;
        availableParents.push(node);
      } 
    } 
    int rootIndex = 0;
    SparkOpJson rootNode = preorder.get(rootIndex);
    return rootNode;
  }
  
  public static SparkOpJson getDagFromPreorder(List<SparkOpJson> preorder) {
    if (preorder.isEmpty())
      return null; 
    Stack<StackElement> availableParents = new Stack<>();
    for (int i = 0; i < preorder.size(); i++) {
      SparkOpJson node = preorder.get(i);
      node.setIdentifier(i + 1);
      StackElement parent = null;
      if (!availableParents.isEmpty())
        parent = availableParents.pop(); 
      if (parent != null) {
        parent.getOperator().addChild(node);
        parent.incrementAssignment();
      } 
      if (parent != null && parent.hasUnassignedChildren())
        availableParents.push(parent); 
      StackElement newNode = new StackElement(node);
      if (newNode.hasUnassignedChildren())
        availableParents.push(newNode); 
    } 
    int rootIndex = 0;
    SparkOpJson rootNode = preorder.get(rootIndex);
    return rootNode;
  }
  
  public static void preOrder(SparkOpJson root, List<SparkOpJson> preorderList) {
    if (root == null)
      return; 
    preorderList.add(root);
    int numChildren = root.numChildren;
    if (numChildren >= 1)
      preOrder((SparkOpJson)root.children.get(0), preorderList); 
    if (numChildren == 2)
      preOrder((SparkOpJson)root.children.get(1), preorderList); 
  }
}
