import React, { ReactElement } from 'react';
import { AxisStackGroups } from '../util/ChartUtils';
import { AxisType, CategoricalChartOptions, DataKey, LayoutType, Margin, StackOffsetType } from '../util/types';
import { AxisMap, CategoricalChartState } from './types';
export interface MousePointer {
    pageX: number;
    pageY: number;
}
export type GraphicalItem<Props = Record<string, any>> = ReactElement<Props, string | React.JSXElementConstructor<Props>> & {
    item: ReactElement<Props, string | React.JSXElementConstructor<Props>>;
};
/**
 * Get the configuration of axis by the options of axis instance
 * @param  {Object} props         Latest props
 * @param {Array}  axes           The instance of axes
 * @param  {Array} graphicalItems The instances of item
 * @param  {String} axisType      The type of axis, xAxis - x-axis, yAxis - y-axis
 * @param  {String} axisIdKey     The unique id of an axis
 * @param  {Object} stackGroups   The items grouped by axisId and stackId
 * @param {Number} dataStartIndex The start index of the data series when a brush is applied
 * @param {Number} dataEndIndex   The end index of the data series when a brush is applied
 * @return {Object}      Configuration
 */
export declare const getAxisMapByAxes: (props: CategoricalChartProps, { axes, graphicalItems, axisType, axisIdKey, stackGroups, dataStartIndex, dataEndIndex, }: {
    axes: ReadonlyArray<ReactElement>;
    graphicalItems: ReadonlyArray<ReactElement>;
    axisType: AxisType;
    axisIdKey: string;
    stackGroups: AxisStackGroups;
    dataStartIndex: number;
    dataEndIndex: number;
}) => AxisMap;
/**
 * Returns default, reset state for the categorical chart.
 * @param {Object} props Props object to use when creating the default state
 * @return {Object} Whole new state
 */
export declare const createDefaultState: (props: CategoricalChartProps) => CategoricalChartState;
export type CategoricalChartFunc = (nextState: CategoricalChartState, event: any) => void;
export interface CategoricalChartProps {
    syncId?: number | string;
    syncMethod?: 'index' | 'value' | Function;
    compact?: boolean;
    width?: number;
    height?: number;
    dataKey?: DataKey<any>;
    data?: any[];
    layout?: LayoutType;
    stackOffset?: StackOffsetType;
    throttleDelay?: number;
    margin?: Margin;
    barCategoryGap?: number | string;
    barGap?: number | string;
    barSize?: number | string;
    maxBarSize?: number;
    style?: any;
    className?: string;
    children?: any;
    defaultShowTooltip?: boolean;
    onClick?: CategoricalChartFunc;
    onMouseLeave?: CategoricalChartFunc;
    onMouseEnter?: CategoricalChartFunc;
    onMouseMove?: CategoricalChartFunc;
    onMouseDown?: CategoricalChartFunc;
    onMouseUp?: CategoricalChartFunc;
    reverseStackOrder?: boolean;
    id?: string;
    startAngle?: number;
    endAngle?: number;
    cx?: number | string;
    cy?: number | string;
    innerRadius?: number | string;
    outerRadius?: number | string;
    title?: string;
    desc?: string;
    accessibilityLayer?: boolean;
    role?: string;
    tabIndex?: number;
}
export declare const generateCategoricalChart: ({ chartName, GraphicalChild, defaultTooltipEventType, validateTooltipEventTypes, axisComponents, legendContent, formatAxisMap, defaultProps, }: CategoricalChartOptions) => (props: CategoricalChartProps) => React.JSX.Element;
