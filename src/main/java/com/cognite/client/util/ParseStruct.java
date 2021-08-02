package com.cognite.client.util;

import com.google.common.base.Preconditions;
import com.google.protobuf.Struct;

import java.util.Collections;
import java.util.List;

/**
 * This class hosts methods to help parse data from {@code Struct} objects. {@code Struct} is used to represent
 * data in CDF Raw as well as the typed version of Json objects.
 */
public class ParseStruct {

    /**
     * Parses a given node
     * @param struct
     * @param path
     * @param delimiter
     * @return
     */
    public static String parseDelimitedString(Struct struct, String path, String delimiter) {
        Preconditions.checkNotNull(struct, "Struct cannot be null");
        Preconditions.checkArgument(null != path && path.isBlank(),
                "Node cannot be null or empty");
        Preconditions.checkArgument(null != delimiter && delimiter.isBlank(),
                "Delimiter cannot be null or empty");

        return "";
    }

    /**
     * Parses a node in the {@code Struct} into a {@code List<String>}. This is convenient in case the node itself,
     * or one of its parents, is a list.
     *
     * @param struct The Struct to parse.
     * @param path The path of node to parse, separated by period ("."). Ex: "parent.child.grandChild"
     * @return A list of the data matching the path. If no match, then an empty list is returned.
     */
    public static List<String> parseStringList(Struct struct, String path) {
        Preconditions.checkNotNull(struct, "Struct cannot be null");
        Preconditions.checkArgument(null != path && path.isBlank(),
                "Node cannot be null or empty");

        return Collections.emptyList();
    }


    /*

    fields {
    key: "to_ActiveSystemStatus"
    value {
      struct_value {
        fields {
          key: "results"
          value {
            list_value {
              values {
                struct_value {
                  fields {
                    key: "IsUserStatus"
                    value {
                      string_value: ""
                    }
                  }
                  fields {
                    key: "StatusCode"
                    value {
                      string_value: "I0072"
                    }
                  }
                  fields {
                    key: "StatusName"
                    value {
                      string_value: "Notification completed"
                    }
                  }
                  fields {
                    key: "StatusObject"
                    value {
                      string_value: "QM000010000010"
                    }
                  }
                  fields {
                    key: "StatusProfile"
                    value {
                      string_value: ""
                    }
                  }
                  fields {
                    key: "StatusSequenceNumber"
                    value {
                      string_value: "00"
                    }
                  }
                  fields {
                    key: "StatusShortName"
                    value {
                      string_value: "NOCO"
                    }
                  }
                  fields {
                    key: "__metadata"
                    value {
                      struct_value {
                        fields {
                          key: "id"
                          value {
                            string_value: "OTIFICATION_SRV/C_StsObjActiv072\')"
                          }
                        }
                        fields {
                          key: "type"
                          value {
                            string_value: "EAM_OBJPG_MAINTNOTIFICATION_SRV.C_StsObjActiveStatusCodeTextType"
                          }
                        }
                        fields {
                          key: "uri"
                          value {
                            string_value: "https://CodeText(StatusObject=\'QM000010000010\',StatusCode=\'I0072\')"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

     */
}
