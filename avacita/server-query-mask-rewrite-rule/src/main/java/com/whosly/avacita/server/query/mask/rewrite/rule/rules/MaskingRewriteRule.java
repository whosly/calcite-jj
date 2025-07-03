package com.whosly.avacita.server.query.mask.rewrite.rule.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MaskingRewriteRule extends RelOptRule {
    private static final Logger LOG = LoggerFactory.getLogger(MaskingRewriteRule.class);
    
    private final MaskingConfigMeta maskingConfigMeta;

    public MaskingRewriteRule(RelOptRuleOperand operand, MaskingConfigMeta maskingConfigMeta) {
        super(operand, "MaskingRewriteRule");
        this.maskingConfigMeta = maskingConfigMeta;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalProject project = call.rel(0);
        final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        final List<RexNode> oldProjects = project.getProjects();
        final List<RexNode> newProjects = new ArrayList<>();

        boolean changed = false;
        for (int i = 0; i < oldProjects.size(); i++) {
            RexNode projectExpr = oldProjects.get(i);
            
            if (projectExpr instanceof RexInputRef) {
                RexInputRef inputRef = (RexInputRef) projectExpr;
                RelDataTypeField field = project.getInput().getRowType().getFieldList().get(inputRef.getIndex());
                String columnName = field.getName();

                // Find the origin of the column
                ColumnOrigin origin = findColumnOrigin(project.getInput(), inputRef.getIndex());
                
                if (origin != null) {
                    MaskingRuleConfig rule = maskingConfigMeta.getRule(origin.getSchemaName(), origin.getTableName(), columnName);
                    if (rule != null) {
                        LOG.debug("Applying rule {} on column {}", rule.getRuleType(), origin.getQualifiedName() + "." + columnName);
                        RexNode newExpr = rule.apply(rexBuilder, projectExpr, field.getType());
                        if (newExpr != projectExpr) {
                            // Ensure the new expression has a correct alias
                            newProjects.add(rexBuilder.makeCall(project.getCluster().getTypeFactory(),
                                    rexBuilder.getRexBuilder().getRexExecutor().getCastFunction(field.getType()),
                                    List.of(newExpr)));
                            changed = true;
                        } else {
                            newProjects.add(projectExpr);
                        }
                    } else {
                        newProjects.add(projectExpr);
                    }
                } else {
                    LOG.warn("Could not find origin for column: {}", columnName);
                    newProjects.add(projectExpr);
                }
            } else {
                // Not a direct column reference, add as is.
                // More complex expressions could be parsed here if needed.
                newProjects.add(projectExpr);
            }
        }

        if (changed) {
            final Project newProject = project.copy(project.getTraitSet(), project.getInput(), newProjects, project.getRowType());
            call.transformTo(newProject);
        }
    }
    
    private ColumnOrigin findColumnOrigin(RelNode node, int columnIndex) {
        if (node instanceof TableScan) {
            List<String> names = ((TableScan) node).getTable().getQualifiedName();
            if (names.size() >= 2) {
                return new ColumnOrigin(names.get(0), names.get(1));
            }
        }

        if (node instanceof Join) {
            Join join = (Join) node;
            int leftFieldCount = join.getLeft().getRowType().getFieldCount();
            if (columnIndex < leftFieldCount) {
                return findColumnOrigin(join.getLeft(), columnIndex);
            } else {
                return findColumnOrigin(join.getRight(), columnIndex - leftFieldCount);
            }
        }

        if (node instanceof Project) {
            Project project = (Project) node;
            RexNode rexNode = project.getProjects().get(columnIndex);
            if (rexNode instanceof RexInputRef) {
                return findColumnOrigin(project.getInput(), ((RexInputRef) rexNode).getIndex());
            }
        }

        // Could be other node types like Filter, Aggregate, etc.
        // For simplicity, we only trace through Project and Join.
        // If the input is not a leaf, we need to recurse on its inputs.
        if (!node.getInputs().isEmpty()) {
            // This simple model assumes a 1-to-1 mapping of columns, which is not always true.
            // A more robust implementation might be needed for complex queries.
            return findColumnOrigin(node.getInput(0), columnIndex);
        }

        return null;
    }

    private static class ColumnOrigin {
        private final String schemaName;
        private final String tableName;

        public ColumnOrigin(String schemaName, String tableName) {
            this.schemaName = schemaName;
            this.tableName = tableName;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public String getTableName() {
            return tableName;
        }

        public String getQualifiedName() {
            return schemaName + "." + tableName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ColumnOrigin that = (ColumnOrigin) o;
            return Objects.equals(schemaName, that.schemaName) &&
                   Objects.equals(tableName, that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaName, tableName);
        }
    }
} 