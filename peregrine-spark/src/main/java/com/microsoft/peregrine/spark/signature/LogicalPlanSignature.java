package com.microsoft.peregrine.spark.signature;

import com.microsoft.peregrine.core.signatures.Signature;
import com.microsoft.peregrine.core.signatures.hash.SignHash64;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import scala.Array;
import scala.Function0;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.Set;
import scala.collection.immutable.Map;
import scala.collection.immutable.Set;
import scala.runtime.AbstractFunction0;

public abstract class LogicalPlanSignature implements Signature<LogicalPlan> {
  public static final int MAX_TO_STRING_FIELDS = 10000;
  
  private static LogicalPlanSignature _hts;
  
  private static LogicalPlanSignature _ht;
  
  public String getSignature(LogicalPlan node) {
    String signature = getLocalSignature(node);
    List<LogicalPlan> children = (List<LogicalPlan>)JavaConverters.seqAsJavaListConverter(node.children()).asJava();
    for (LogicalPlan child : children)
      signature = SignHash64.Compute(signature, getSignature(child)); 
    return signature;
  }
  
  public String getLocalSignature(LogicalPlan node) {
    String sig = SignHash64.Compute(node.nodeName());
    if (node.nodeName().contains("LogicalRelation")) {
      Option<CatalogTable> option = ((LogicalRelation)node).catalogTable();
      if (!option.isEmpty()) {
        CatalogTable catalogTable = (CatalogTable)option.getOrElse(null);
        if (catalogTable != null)
          sig = SignHash64.Compute(sig, getArgSignature(catalogTable)); 
      } 
    } 
    Iterator<Object> argsJava = (Iterator<Object>)JavaConverters.asJavaIteratorConverter(node.stringArgs()).asJava();
    while (argsJava.hasNext()) {
      Object argument = argsJava.next();
      sig = SignHash64.Compute(sig, getArgSignature(argument));
    } 
    return sig;
  }
  
  private String getArgSignature(Object arg) {
    if (Literal.class.isAssignableFrom(arg.getClass()))
      return getLiteralSignature((Literal)arg); 
    if (CatalogTable.class.isAssignableFrom(arg.getClass()))
      return getCatalogTableSignature((CatalogTable)arg); 
    if (AttributeReference.class.isAssignableFrom(arg.getClass())) {
      AttributeReference attrRef = (AttributeReference)arg;
      return SignHash64.Compute(attrRef.name());
    } 
    if (Expression.class.isAssignableFrom(arg.getClass())) {
      Expression canExpr = (Expression)arg;
      String treeSig = SignHash64.Compute(canExpr.nodeName());
      List<Expression> children = (List<Expression>)JavaConverters.seqAsJavaListConverter(canExpr.children()).asJava();
      if (children == null || children.size() == 0)
        return treeSig; 
      for (Expression child : children)
        treeSig = SignHash64.Compute(treeSig, getArgSignature(child)); 
      return treeSig;
    } 
    if (Array.class.isAssignableFrom(arg.getClass())) {
      Object[] javaArray = (Object[])arg;
      String arraySig = null;
      for (Object element : javaArray)
        arraySig = SignHash64.Compute(arraySig, getArgSignature(element)); 
      return arraySig;
    } 
    if (Seq.class.isAssignableFrom(arg.getClass())) {
      List<Object> javaList = (List<Object>)JavaConverters.seqAsJavaListConverter((Seq)arg).asJava();
      String listSig = null;
      for (Object element : javaList)
        listSig = SignHash64.Compute(listSig, getArgSignature(element)); 
      return listSig;
    } 
    if (Set.class.isAssignableFrom(arg.getClass())) {
      Set<Object> javaSet = (Set<Object>)JavaConverters.setAsJavaSetConverter((Set)arg).asJava();
      String setSig = null;
      for (Object element : javaSet)
        setSig = SignHash64.Compute(setSig, getArgSignature(element)); 
      return setSig;
    } 
    if (TreeNode.class.isAssignableFrom(arg.getClass())) {
      TreeNode treeNode = (TreeNode)arg;
      String treeSig = SignHash64.Compute(treeNode.nodeName());
      List<LogicalPlan> children = (List<LogicalPlan>)JavaConverters.seqAsJavaListConverter(treeNode.children()).asJava();
      if (children == null || children.size() == 0)
        return treeSig; 
      for (LogicalPlan child : children)
        treeSig = SignHash64.Compute(treeSig, getArgSignature(child)); 
      return treeSig;
    } 
    return null;
  }
  
  public static String getSignatures(LogicalPlan node) {
    StringBuffer sb = new StringBuffer();
    sb.append("HTS:");
    sb.append(HTS(node));
    sb.append(",HT:");
    sb.append(HT(node));
    return sb.toString();
  }
  
  public static String HTS(LogicalPlan node) {
    if (_hts == null)
      _hts = new LogicalPlanSignature() {
          public String getLiteralSignature(Literal literal) {
            return SignHash64.Compute(literal.simpleString());
          }
          
          public String getCatalogTableSignature(CatalogTable catalogTable) {
            StringBuilder versionedTableIdentifer = new StringBuilder();
            versionedTableIdentifer.append(catalogTable.identifier().toString());
            versionedTableIdentifer.append(catalogTable.createTime());
            Map<String, String> properties = catalogTable.properties();
            String ddlTime = LogicalPlanSignature.getValueFromMap("transient_lastDdlTime", properties);
            versionedTableIdentifer.append(ddlTime);
            Map<String, String> ignoredProperties = catalogTable.ignoredProperties();
            String numFiles = LogicalPlanSignature.getValueFromMap("numFiles", ignoredProperties);
            versionedTableIdentifer.append(numFiles);
            String totalSize = LogicalPlanSignature.getValueFromMap("totalSize", ignoredProperties);
            versionedTableIdentifer.append(totalSize);
            return SignHash64.Compute(versionedTableIdentifer.toString());
          }
        }; 
    return _hts.getSignature(node);
  }
  
  private static String getValueFromMap(String key, Map<String, String> props) {
    String value = (String)props.getOrElse(key, (Function0)new AbstractFunction0<String>() {
          public String apply() {
            return "";
          }
        });
    return value;
  }
  
  public static String HT(LogicalPlan node) {
    if (_ht == null)
      _ht = new LogicalPlanSignature() {
          public String getLiteralSignature(Literal literal) {
            return "";
          }
          
          public String getCatalogTableSignature(CatalogTable catalogTable) {
            return SignHash64.Compute(catalogTable.identifier().toString());
          }
        }; 
    return _ht.getSignature(node);
  }
  
  public abstract String getLiteralSignature(Literal paramLiteral);
  
  public abstract String getCatalogTableSignature(CatalogTable paramCatalogTable);
}
