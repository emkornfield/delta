/*
 * Copyright (2025) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.internal.util;

import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.types.*;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * SchemaChanges encapsulates a list of added, removed, renamed, or updated fields in a schema
 * change. Updates include renamed fields, reordered fields, type changes, nullability changes, and
 * metadata attribute changes. This set of updates can apply to nested fields within structs. In
 * case any update is applied to a nested field, an update will be produced for every level of
 * nesting. This includes re-ordered columns in a nested field. Note that SchemaChanges does not
 * capture re-ordered columns in top level schema.
 *
 * <p>For example, given a field struct_col: struct<inner_struct<id: int>> if id is renamed to
 * `renamed_id` 1 update will be produced for the change to struct_col and 1 update will be produced
 * for the change to inner_struct
 *
 * <p>ToDo: Possibly track moves/renames independently, enable capturing re-ordered columns in top
 * level schema
 */
class SchemaChanges {
  private List<StructField> addedFields;
  private List<StructField> removedFields;
  private List<Tuple2<StructField, StructField>> updatedFields;

  private SchemaChanges(
      List<StructField> addedFields,
      List<StructField> removedFields,
      List<Tuple2<StructField, StructField>> updatedFields) {
    this.addedFields = Collections.unmodifiableList(addedFields);
    this.removedFields = Collections.unmodifiableList(removedFields);
    this.updatedFields = Collections.unmodifiableList(updatedFields);
  }

  static class Builder {
    private List<StructField> addedFields = new ArrayList<>();
    private List<StructField> removedFields = new ArrayList<>();
    private List<Tuple2<StructField, StructField>> updatedFields = new ArrayList<>();

    public Builder withAddedField(StructField addedField) {
      addedFields.add(addedField);
      return this;
    }

    public Builder withRemovedField(StructField removedField) {
      removedFields.add(removedField);
      return this;
    }

    public Builder withUpdatedField(StructField existingField, StructField newField) {
      updatedFields.add(new Tuple2<>(existingField, newField));
      return this;
    }

    public SchemaChanges build() {
      return new SchemaChanges(addedFields, removedFields, updatedFields);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  /* Added Fields */
  public List<StructField> addedFields() {
    return addedFields;
  }

  /* Removed Fields */
  public List<StructField> removedFields() {
    return removedFields;
  }

  /* Updated Fields (e.g. rename, type change) represented as a Tuple<FieldBefore, FieldAfter> */
  public List<Tuple2<StructField, StructField>> updatedFields() {
    return updatedFields;
  }

  public StructType recordTypeWidenedFields(StructType schema) {
    Map<StructField, Tuple2<StructField, StructField>> newFieldToWidenedChange =
            updatedFields.stream().filter(t -> t._1.getDataType() instanceof BasePrimitiveType &&
                    !t._1.getDataType().equivalent(t._2.getDataType()))
                    .collect(Collectors.toMap(t -> t._2, t -> t));
    if (newFieldToWidenedChange.isEmpty()) {
      return schema;
    }
    return updateWithZipper(schema);
  }

    /**
     * Interface for manipulating Schema's.
     *
     * 'Zipper' is the name of a functional data structure that allows for manipulating
     * points in "immutable" data-structures such as trees.
     * This approach is used because it allows for relatively easy non-recursive implementation
     * of the tree traversal and mutation of Metadata.
     *
     * There is only one implementation of the interface but it is kept separate to ease clarify
     * in reading.
     */
  private interface SchemaZipper {
        /**
         * Returns true if there are any children of the current zipper (i.e. the field
         * is a nested type).
         */
      boolean hasChildren();
        /**
         * Returns a new zipper that represents children of the current schema field.
         */
      SchemaZipper childrenZipper();
      boolean hasNoMoreSiblings();
      SchemaZipper moveToSibling();
      /**
         * Returns a new zipper that represents the parent of the current schema field.
       * Will return null if there is no parent.
         */
      SchemaZipper moveToParent();

        /**
         * Returns the current field the zipper is targetted at.
         */
      StructField currentField();

        /**
         * Replace the current targeted field with a new field.
         */
      void updateField(StructField structField );

      /**
        * Returns the DataType represented by all siblings at this level of the zipper.
        */
      DataType extractDataTypeFromFields();

      static SchemaZipper createZipper(StructType schema) {
          return createZipper(/*parent=*/null, schema);
      }

      static SchemaZipper createZipper(SchemaZipper parent, DataType type) {
          if (type instanceof BasePrimitiveType) {
              return new SchemaZipperImpl(parent, fields->fields.get(0).getDataType(), Collections.singletonList(field));
          } else if (type instanceof ArrayType) {
              ArrayType arrayType = (ArrayType) type;
              return new SchemaZipperImpl(parent, fields->new ArrayType(fields.get(0)), Collections.singletonList(arrayType.getElementField()));
          } else if (type instanceof MapType) {
              MapType mapType = (MapType) type;
              return new SchemaZipperImpl(parent, fields->new MapType(fields.get(0), fields.get(1)),
                      Arrays.asList(mapType.getKeyField(), mapType.getValueField()));
          } else if (type instanceof StructType) {
              StructType structType = (StructType)type;
              return new SchemaZipperImpl(parent, fields->new StructType(fields), structType.fields());
          } else {
              throw new KernelException("Unsupported data type: " + type);
          }
      }

      static SchemaZipper createZipper(SchemaZipper parent, StructField field) {
          if (field.getDataType() instanceof BasePrimitiveType) {
              return new SchemaZipperImpl(parent, fields->fields.get(0).getDataType(), Collections.singletonList(field));
          } else if (field.getDataType() instanceof ArrayType) {
              ArrayType arrayType = (ArrayType) field.getDataType();
              return new SchemaZipperImpl(parent, fields->new ArrayType(fields.get(0)), Collections.singletonList(arrayType.getElementField()));
          } else if (field.getDataType() instanceof MapType) {
              MapType mapType = (MapType) field.getDataType();
              return new SchemaZipperImpl(parent, fields->new MapType(fields.get(0), fields.get(1)),
                      Arrays.asList(mapType.getKeyField(), mapType.getValueField()));
          } else if (field.getDataType() instanceof StructType) {
              StructType structType = (StructType) field.getDataType();
              return new SchemaZipperImpl(parent, fields->new StructType(fields), structType.fields());
          } else {
              throw new KernelException("Unsupported data type: " + field.getDataType());
          }
      }
  }

  /**
   * Implementation of the SchemaZipper interface.
   *
   * This implementation differs from the canonical
   * approach because it allows internal mutability of the zipper which avoids some amount
   * of creation and suitable for the initial use-cases.
   */
    private static class SchemaZipperImpl implements SchemaZipper {
        private final SchemaZipper parent;
        private final Function<List<StructField>, DataType> constructType;
        private final List<StructField> fields;
        private int index = 0;
        boolean modified = false;

        public SchemaZipperImpl(SchemaZipper parent, Function<List<StructField>, DataType> constructType, List<StructField> fields) {
            this.parent = parent;
            this.constructType = constructType;
            this.fields = fields;
        }

        public boolean hasChildren() {
            return !(currentField().getDataType() instanceof BasePrimitiveType);
        }

        @Override
        public SchemaZipper childrenZipper() {
            if (!hasChildren()) {
                return null;
            }
           return SchemaZipper.createZipper(this, fields.get(index));
        }

        @Override
        public SchemaZipper moveToSibling() {
            if (hasNoMoreSiblings()) {
                return null;
            }
            index++;
            return this;
        }

        @Override
        public boolean hasNoMoreSiblings() {
            return index >= fields.size() - 1;
        }

        @Override
        public SchemaZipper moveToParent() {
            if (parent == null || !modified) {
                return parent;
            }
            parent.updateField(parent.currentField().withNewDataType(
                   extractDataTypeFromFields());
            return parent;
        }

        @Override
        public DataType extractDataTypeFromFields() {
            return constructType.apply(fields);
        }

        @Override
        public StructField currentField() {
            return fields.get(index);
        }

        @Override
        public void updateField(StructField structField) {
            fields.set(index, structField);
            modified = true;
        }
    }

  private StructType updateWithZipper(StructType schema,Map<StructField, Tuple2<StructField, StructField>> newFieldToWidenedChange ) {
      SchemaZipper zipper = SchemaZipper.createZipper(schema);
      SchemaZipper previousZipper = null;
      boolean finishedVisitingZipper = false;

      while (zipper != null) {
         previousZipper = zipper;
         if (finishedVisitingZipper) {
             // A node is already visited implies that it was already visited on the way down
             // this means we have effectively popped the stack.  Which means the only options
             // are to either move right or back up. When moving backup we ensure we update
             // the field if necessary with new metadata.
             if (zipper.hasNoMoreSiblings()) {
               Tuple2<StructField, StructField> widenedChange = newFieldToWidenedChange.get(zipper.currentField());
               if (widenedChange != null) {
                   // Note for maps and arrays, this records the type change on there elements.
                   // StructFields are not actually modelled in the spec this way (only types
                   // are recorded).  When a new StructType is created it pulls up any type changes
                   // that wouldn't be persistable in JSON.
                  FieldMetadata newMetadata =
                  updateMetadata(zipper.currentField().getMetadata(), widenedChange);
                  zipper.updateField(zipper.currentField().withNewMetadata(newMetadata));
               }
               zipper = zipper.moveToParent();
             } else {
               zipper = zipper.moveToSibling();
               finishedVisitingZipper = false;
             }
         } else {
             if (zipper.hasChildren()) {
                 zipper = zipper.childrenZipper();
             } else {
                 finishedVisitingZipper = true;
             }
          }
         }
         return (StructType)previousZipper.extractDataTypeFromFields();
      }

    private FieldMetadata updateMetadata(FieldMetadata metadata, Tuple2<StructField, StructField> widenedChange) {
        FieldMetadata.Builder newChange = FieldMetadata.builder();
        newChange.putString("fromType", DataTypeJsonSerDe.serializeDataType(widenedChange._1.getDataType()));
        newChange.putString("toType", DataTypeJsonSerDe.serializeDataType(widenedChange._2.getDataType()));


        FieldMetadata changes[] = metadata.getMetadataArray("delta.typeChanges");
        List<FieldMetadata> newChanges = new ArrayList<>(changes == null ? 1 : changes.length() + 1);
        if (changes != null) {
            newChanges.addAll(Arrays.asList(changes));
        }
        newChanges.add(newChange.build());

        FieldMetadata.Builder builder = FieldMetadata.builder().fromMetadata(metadata);
        builder.putFieldMetadataArray("delta.typeChanges", newChanges.toArray(new FieldMetadata[0]));
        return builder.build();
    }

}
